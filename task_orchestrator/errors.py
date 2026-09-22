"""业务异常：每类拒绝原因可通过 error_code / HTTP 状态码区分。"""

from __future__ import annotations


class OrchestratorError(Exception):
    """所有编排器业务异常的基类。"""

    http_status = 400
    error_code = "bad_request"


class InvalidRequest(OrchestratorError):
    """参数非法（字段缺失、类型/范围错误、未知任务类型）。"""

    http_status = 422
    error_code = "invalid_request"


class PayloadConflict(OrchestratorError):
    """同一业务唯一键已存在，但载荷不一致。"""

    http_status = 409
    error_code = "payload_conflict"


class TaskNotFound(OrchestratorError):
    """任务不存在。"""

    http_status = 404
    error_code = "task_not_found"


class AlreadyTerminal(OrchestratorError):
    """任务已处于终态，取消等转换不可再发生。"""

    http_status = 409
    error_code = "already_terminal"


class RetryableTaskError(Exception):
    """任务处理器抛出的可重试失败。"""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class NonRetryableTaskError(Exception):
    """任务处理器抛出的不可重试失败（直接进入 FAILED）。"""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
