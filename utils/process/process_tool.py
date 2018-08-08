# -*- coding: UTF-8 -*-


def process_assert(code):
    if code != 0:
        raise SystemExit("Process aborted.")
