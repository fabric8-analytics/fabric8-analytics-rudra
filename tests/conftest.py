#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Add import context."""

from pathlib import Path
import sys

src = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(src))
