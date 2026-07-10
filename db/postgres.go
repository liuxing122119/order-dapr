// Dapr多运行时云原生应用实践 - 数据库连接管理模块
// 功能：PostgreSQL连接池初始化、配置参数调优、资源释放与优雅关闭
package db

import (
	"database/sql" // SQL数据库操作接口标准库，提供通用的数据库访问能力
	"fmt"          // 格式化输入输出库用于错误消息构造
	"time"         // 时间处理工具用于连接生命周期配置

	_ "github.com/lib/pq" // PostgreSQL驱动程序导入（仅注册驱动无需直接引用）
)

var DB *sql.DB // 全局数据库连接池实例指针，供所有服务模块共享使用

// InitDB 初始化PostgreSQL数据库连接池并配置连接参数优化性能
// 建立与订单数据库的持久化通道，配置连接池大小和超时参数防止资源耗尽
// 返回值：初始化过程中的错误信息（通常为nil表示成功）
func InitDB() error {
	// 构造PostgreSQL连接字符串包含所有必需的认证和网络参数
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=orderdb sslmode=disable"
	// 参数说明：
	// host=localhost - 数据库服务器主机地址（本地开发环境）
	// port=5432 - PostgreSQL默认监听端口号
	// user=postgres - 数据库登录用户名
	// password=postgres - 数据库登录密码（生产环境应使用环境变量或密钥管理）
	// dbname=orderdb - 目标数据库名称（订单业务专用数据库）
	// sslmode=disable - SSL加密模式（开发环境禁用，生产环境应启用verify-full）

	var err error // 定义错误变量用于接收可能发生的初始化错误
	// 调用sql.Open创建数据库连接池实例（此时不建立实际连接，延迟到首次使用时）
	DB, err = sql.Open("postgres", connStr) // 使用postgres驱动名和连接字符串
	if err != nil {
		// 连接池创建失败时返回包装后的详细错误信息便于排查问题
		return fmt.Errorf("failed to open database connection: %w", err) // 错误包装保留原始信息
	}

	// 配置连接池最大打开连接数限制（防止过多连接耗尽数据库资源）
	DB.SetMaxOpenConns(25) // 最大允许同时打开的数据库连接数（根据CPU核心数和并发量调整）
	// 配置连接池最大空闲连接数（保持一定数量的热连接提高响应速度）
	DB.SetMaxIdleConns(10) // 连接空闲时保留在池中的最小连接数（减少重新建立连接的开销）
	// 配置单个连接的最大生存时间（定期回收长时间使用的连接避免连接泄漏）
	DB.SetConnMaxLifetime(5 * time.Minute) // 连接存活超过5分钟后自动关闭重建（适应网络波动）

	// 执行数据库ping测试验证连接字符串的有效性和网络连通性
	if err = DB.Ping(); err != nil {
		// Ping测试失败时返回明确的数据库不可达错误提示
		return fmt.Errorf("failed to ping database: %w", err) // 错误包装保留原始信息
	}

	return nil // 初始化完成且Ping测试通过时返回nil表示无错误
}

// CloseDB 优雅关闭数据库连接池释放所有占用的系统资源
// 在应用程序退出或需要重新初始化时调用此函数确保资源正确释放
func CloseDB() {
	// 检查数据库连接池实例是否已初始化（防止空指针异常）
	if DB != nil {
		// 调用Close方法关闭所有活跃连接并释放连接池资源
		DB.Close() // 关闭后DB变量置为nil状态不可再使用
	}
}
