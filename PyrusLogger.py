import pyrus.models.requests


class PyrusLogger:
    pyrus_client = None
    task_id = None

    def __init__(self, pyrus_client, task_id):
        self.pyrus_client = pyrus_client
        self.task_id = task_id

    def log(self, text):
        self.pyrus_client.comment_task(self.task_id, task_comment_request=pyrus.models.requests.TaskCommentRequest(
            text=text))
        return
