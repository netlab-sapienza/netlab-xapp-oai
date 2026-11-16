"""Nonblocking framed transport and monotonic 100 ms scheduling primitives.

Neither receive nor a partial send waits for a complete frame. One outbound
command is allowed at a time, with an explicit deadline. There is no database
access here. This is soft real time: scheduling lateness is measured, not hidden.
"""
import select
import socket
import struct
import time

MAX_FRAME=1024*1024


class FramedIO:
    def __init__(self,sock):
        self.sock=sock
        sock.setblocking(False)
        self.input=bytearray();self.output=b'';self.command=None;self.deadline=None

    def queue(self,payload,command,now):
        if self.command is not None:raise ValueError('command already in flight')
        if not isinstance(payload,bytes) or not 0<len(payload)<=MAX_FRAME:
            raise ValueError('invalid outbound frame')
        self.output=struct.pack('!I',len(payload))+payload
        self.command=command;self.deadline=now+.5

    def ready(self,timeout):
        return select.select([self.sock],[self.sock] if self.output else [],[],max(0.,timeout))[:2]

    def receive(self):
        try:data=self.sock.recv(65536)
        except BlockingIOError:return []
        if not data:raise ConnectionError('connector closed the control stream')
        self.input.extend(data)
        frames=[]
        while len(self.input)>=4:
            length=struct.unpack('!I',self.input[:4])[0]
            if not 0<length<=MAX_FRAME:raise ValueError('invalid inbound frame length')
            if len(self.input)<length+4:break
            frames.append(bytes(self.input[4:length+4]));del self.input[:length+4]
        if len(self.input)>MAX_FRAME+4:raise ValueError('receive buffer exceeds frame bound')
        return frames

    def flush(self,now):
        if self.command is None:return None
        if now>self.deadline:raise TimeoutError('control send deadline exceeded; application unknown')
        try:n=self.sock.send(self.output)
        except BlockingIOError:return None
        if n<=0:raise ConnectionError('control send failed')
        self.output=self.output[n:]
        if self.output:return None
        result=self.command
        self.command=None;self.deadline=None
        return result


class Ticker:
    def __init__(self,now,interval=.1):
        if interval!=.1:raise ValueError('experimental runtime requires 100 ms polling')
        self.interval=interval;self.next=now;self.last=None

    def due(self,now):
        if now+1e-9<self.next:return None
        planned=self.next
        skipped=max(0,int((now-planned+1e-9)//self.interval))
        self.next=planned+(skipped+1)*self.interval
        # Never execute a burst of artificial catch-up polls.
        result=dict(planned_mono=planned,actual_mono=now,late_s=max(0.,now-planned),
                    missed_timer_slots=skipped,
                    elapsed_since_poll_s=None if self.last is None else now-self.last)
        self.last=now
        return result


def run_loop(io,on_report,on_poll,on_sent,should_stop,clock=time.monotonic):
    ticker=Ticker(clock())
    while not should_stop():
        now=clock()
        if io.command is not None and now>io.deadline:
            raise TimeoutError('control send deadline exceeded; application unknown')
        tick=ticker.due(now)
        if tick is not None:on_poll(tick)
        if should_stop():break
        timeout=min(.1,max(0.,ticker.next-clock()))
        if io.command is not None:timeout=min(timeout,max(0.,io.deadline-clock()))
        readable,writable=io.ready(timeout)
        if readable:
            for frame in io.receive():on_report(frame,clock())
        if writable:
            sent=io.flush(clock())
            if sent is not None:on_sent(sent,clock())
