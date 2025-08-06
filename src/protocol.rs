use bytes::{Bytes, BytesMut, BufMut};
use std::net::SocketAddr;

/// PROXY Protocol v2 实现
pub struct ProxyProtocolV2;

#[derive(Debug, Clone)]
pub struct ProxyHeader {
    pub src_addr: SocketAddr,
    pub dst_addr: SocketAddr,
    pub protocol: Protocol,
}

#[derive(Debug, Clone)]
pub enum Protocol {
    TCP,
    UDP,
}

impl ProxyProtocolV2 {
    /// PROXY Protocol v2 签名
    const SIGNATURE: &'static [u8] = b"\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A";

    /// 生成 PROXY Protocol v2 头部
    pub fn generate_header(header: &ProxyHeader) -> Bytes {
        // 提前判断非同栈, 非同栈不符合规定, 不发送Proxy Protocol v2包
        if header.src_addr.is_ipv4() != header.dst_addr.is_ipv4() {
            return Bytes::new();
        }

        let mut buf = BytesMut::new();

        // 添加签名
        buf.put_slice(Self::SIGNATURE);

        // 版本和命令 (版本2, PROXY命令)
        buf.put_u8(0x20 | 0x01);

        // 协议和地址族
        let (protocol_byte, addr_len) = match (&header.src_addr, &header.protocol) {
            (SocketAddr::V4(_), Protocol::TCP) => (0x11, 12), // TCP over IPv4
            (SocketAddr::V6(_), Protocol::TCP) => (0x21, 36), // TCP over IPv6
            (SocketAddr::V4(_), Protocol::UDP) => (0x12, 12), // UDP over IPv4
            (SocketAddr::V6(_), Protocol::UDP) => (0x22, 36), // UDP over IPv6
        };
        buf.put_u8(protocol_byte);

        // 地址长度
        buf.put_u16(addr_len);

        // 源地址和目标地址
        match header.src_addr {
            SocketAddr::V4(src) => {
                buf.put_slice(&src.ip().octets());
                if let SocketAddr::V4(dst) = header.dst_addr {
                    buf.put_slice(&dst.ip().octets());
                    buf.put_u16(src.port());
                    buf.put_u16(dst.port());
                }
            }
            SocketAddr::V6(src) => {
                buf.put_slice(&src.ip().octets());
                if let SocketAddr::V6(dst) = header.dst_addr {
                    buf.put_slice(&dst.ip().octets());
                    buf.put_u16(src.port());
                    buf.put_u16(dst.port());
                }
            }
        }

        buf.freeze()
    }
}