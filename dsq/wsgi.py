import os
from .manager import load_manager
from .http import Application

application = Application(load_manager(os.environ.get('DSQ_TASKS')))
