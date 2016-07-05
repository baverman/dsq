import os
from .utils import load_manager
from .http import Application

application = Application(load_manager(os.environ.get('DSQ_TASKS')))
