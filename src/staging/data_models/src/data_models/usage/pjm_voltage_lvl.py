from enum import Enum


class PjmVoltagelvl(Enum):
    transmission = 0
    sub_transmission = 1
    primary_substation = 2
    primary = 3
    secondary = 4
    unmetered = 5
