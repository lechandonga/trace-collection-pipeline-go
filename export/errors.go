package export

import "errors"

// errExporterShutdown 是导出器关闭后的统一内部错误。
var errExporterShutdown = errors.New("exporter is shutdown")
