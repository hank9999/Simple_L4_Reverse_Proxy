use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::info;

mod config;
mod proxy;
mod protocol;
mod server;
mod udp_proxy;

use config::Config;
use server::ProxyServer;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // 加载配置
    let config = Config::load(&args.config)?;
    info!("配置加载成功: {:?}", config);

    // 启动代理服务器
    let server = ProxyServer::new(config);
    server.run().await?;

    Ok(())
}