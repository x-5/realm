use std::future::Future;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use realm_io::{AsyncRawIO, CopyBuffer, buf_size, pipe_size};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone)]
pub struct ByteCounter {
    endpoint: Arc<AtomicU64>,
    device: Arc<AtomicU64>,
}

impl ByteCounter {
    pub fn new(endpoint: Arc<AtomicU64>, device: Arc<AtomicU64>) -> Self {
        Self { endpoint, device }
    }

    #[inline]
    pub fn add(&self, n: u64) {
        if n > 0 {
            self.endpoint.fetch_add(n, Ordering::Relaxed);
            self.device.fetch_add(n, Ordering::Relaxed);
        }
    }
}

/// AsyncRead + AsyncWrite wrapper that counts bytes on read/write in real time.
pub struct MeteredStream<S> {
    pub inner: S,
    pub rx: ByteCounter,
    pub tx: ByteCounter,
}

impl<S> MeteredStream<S> {
    pub fn new(inner: S, rx: ByteCounter, tx: ByteCounter) -> Self {
        Self { inner, rx, tx }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for MeteredStream<S> {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let res = ready!(Pin::new(&mut this.inner).poll_read(cx, buf));
        if res.is_ok() {
            let delta = buf.filled().len().saturating_sub(before);
            this.rx.add(delta as u64);
        }
        Poll::Ready(res)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for MeteredStream<S> {
    #[inline]
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.tx.add(n as u64);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        iovec: &[IoSlice<'_>],
    ) -> Poll<Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write_vectored(cx, iovec) {
            Poll::Ready(Ok(n)) => {
                this.tx.add(n as u64);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
}

/// Userspace metered bidirectional copy using realm_io's CopyBuffer.
pub async fn bidi_mem_copy_metered<A, B>(
    a: &mut A,
    b: &mut B,
    rx: ByteCounter,
    tx: ByteCounter,
) -> Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let mut metered_a = MeteredStream::new(a, rx, tx);
    let a_to_b_buf = CopyBuffer::new(vec![0u8; buf_size()].into_boxed_slice());
    let b_to_a_buf = CopyBuffer::new(vec![0u8; buf_size()].into_boxed_slice());
    realm_io::bidi_copy_buf(&mut metered_a, b, a_to_b_buf, b_to_a_buf).await
}

/// Userspace metered bidirectional copy reusing existing pre-allocated buffers.
pub async fn bidi_mem_copy_with_bufs<A, B>(
    a: &mut A,
    b: &mut B,
    buf1: Vec<u8>,
    buf2: Vec<u8>,
    rx: ByteCounter,
    tx: ByteCounter,
) -> Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let mut metered_a = MeteredStream::new(a, rx, tx);
    let a_to_b_buf = CopyBuffer::new(buf1);
    let b_to_a_buf = CopyBuffer::new(buf2);
    realm_io::bidi_copy_buf(&mut metered_a, b, a_to_b_buf, b_to_a_buf).await
}

struct MeteredPipe {
    rd: RawFd,
    wr: RawFd,
}

impl MeteredPipe {
    fn new() -> Result<Self> {
        const DF_PIPE_SIZE: usize = 16 * 0x1000;
        let mut fds = [0 as libc::c_int; 2];
        unsafe {
            if libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) < 0 {
                return Err(Error::last_os_error());
            }
            let cur_size = pipe_size();
            if cur_size != DF_PIPE_SIZE {
                libc::fcntl(fds[1], libc::F_SETPIPE_SZ, cur_size);
            }
        }
        Ok(Self {
            rd: fds[0],
            wr: fds[1],
        })
    }
}

impl Drop for MeteredPipe {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.rd);
            libc::close(self.wr);
        }
    }
}

#[inline]
fn splice_n(r: RawFd, w: RawFd, n: usize) -> isize {
    unsafe {
        libc::splice(
            r,
            std::ptr::null_mut::<libc::loff_t>(),
            w,
            std::ptr::null_mut::<libc::loff_t>(),
            n,
            libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK,
        )
    }
}

enum SpliceState {
    Running {
        pipe: MeteredPipe,
        read_done: bool,
        pending_bytes: usize,
        amt: u64,
        counter: ByteCounter,
    },
    ShuttingDown(u64),
    Done(u64),
}

impl SpliceState {
    fn new(pipe: MeteredPipe, counter: ByteCounter) -> Self {
        Self::Running {
            pipe,
            read_done: false,
            pending_bytes: 0,
            amt: 0,
            counter,
        }
    }

    fn poll_transfer<SR, SW>(
        &mut self,
        cx: &mut Context<'_>,
        r: &mut SR,
        w: &mut SW,
    ) -> Poll<Result<u64>>
    where
        SR: AsyncRead + AsyncWrite + AsyncRawIO + Unpin,
        SW: AsyncRead + AsyncWrite + AsyncRawIO + Unpin,
    {
        loop {
            match self {
                SpliceState::Running {
                    pipe,
                    read_done,
                    pending_bytes,
                    amt,
                    counter,
                } => {
                    if *pending_bytes == 0 && !*read_done {
                        let n = match r.poll_read_raw(cx, || {
                            splice_n(r.as_raw_fd(), pipe.wr, isize::MAX as usize)
                        }) {
                            Poll::Ready(Ok(n)) => n,
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => return Poll::Pending,
                        };
                        if n == 0 {
                            *read_done = true;
                        } else {
                            *pending_bytes = n;
                        }
                    }

                    while *pending_bytes > 0 {
                        let written = ready!(w.poll_write_raw(cx, || {
                            splice_n(pipe.rd, w.as_raw_fd(), *pending_bytes)
                        }))?;
                        if written == 0 {
                            return Poll::Ready(Err(ErrorKind::WriteZero.into()));
                        }
                        *pending_bytes -= written;
                        *amt += written as u64;
                        counter.add(written as u64);
                    }

                    if *pending_bytes == 0 && *read_done {
                        let total = *amt;
                        *self = SpliceState::ShuttingDown(total);
                    }
                }
                SpliceState::ShuttingDown(total) => {
                    ready!(Pin::new(&mut *w).poll_shutdown(cx))?;
                    let total = *total;
                    *self = SpliceState::Done(total);
                }
                SpliceState::Done(total) => return Poll::Ready(Ok(*total)),
            }
        }
    }
}

struct BidiSplice<'a, A, B> {
    a: &'a mut A,
    b: &'a mut B,
    a_to_b: SpliceState,
    b_to_a: SpliceState,
}

impl<A, B> Future for BidiSplice<'_, A, B>
where
    A: AsyncRead + AsyncWrite + AsyncRawIO + Unpin,
    B: AsyncRead + AsyncWrite + AsyncRawIO + Unpin,
{
    type Output = Result<(u64, u64)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let a_to_b = this.a_to_b.poll_transfer(cx, this.a, this.b)?;
        let b_to_a = this.b_to_a.poll_transfer(cx, this.b, this.a)?;

        // Brutal shutdown matching realm default behavior
        match (a_to_b, b_to_a) {
            (Poll::Ready(a), Poll::Ready(b)) => Poll::Ready(Ok((a, b))),
            (Poll::Pending, Poll::Ready(b)) => Poll::Ready(Ok((0, b))),
            (Poll::Ready(a), Poll::Pending) => Poll::Ready(Ok((a, 0))),
            _ => Poll::Pending,
        }
    }
}

/// Linux zero-copy splice bidirectional relay with real-time per-write byte counting.
pub async fn bidi_zero_copy_metered<A, B>(
    a: &mut A,
    b: &mut B,
    rx: ByteCounter,
    tx: ByteCounter,
) -> Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + AsyncRawIO + Unpin,
    B: AsyncRead + AsyncWrite + AsyncRawIO + Unpin,
{
    let a_to_b = SpliceState::new(MeteredPipe::new()?, rx);
    let b_to_a = SpliceState::new(MeteredPipe::new()?, tx);
    BidiSplice {
        a,
        b,
        a_to_b,
        b_to_a,
    }
    .await
}
