package common

import (
	"log/slog"
	"os"
)

func init() {
	// 创建 Handler 时指定最低级别为 Debug
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,  // 设置为 Debug 级别
	})

	// 设置全局 logger
	slog.SetDefault(slog.New(handler))
}
