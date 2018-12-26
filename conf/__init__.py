# -*- coding: utf-8 -*-
import os
from config import Config

sysconfig = Config(file(os.path.join(os.path.dirname(__file__),'user_activity.cfg')))
