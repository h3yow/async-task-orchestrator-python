"""本地启动入口：uvicorn main:app"""

from task_orchestrator.app import create_app

app = create_app()
