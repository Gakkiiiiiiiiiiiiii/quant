class QuantDemoError(Exception):
    """系统基础异常。"""


class ConfigError(QuantDemoError):
    """配置异常。"""


class DataNotReadyError(QuantDemoError):
    """数据未就绪异常。"""


class QmtUnavailableError(QuantDemoError):
    """QMT 依赖不可用异常。"""


class RiskRejectedError(QuantDemoError):
    """风控拒绝异常。"""
