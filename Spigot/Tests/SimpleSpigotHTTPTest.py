"""
Simple Spigot Tests for SpigotHTTP.
"""

# Ugly hack to allow absolute import from the root folder.
# noinspection PyUnboundLocalVariable
if __name__ == "__main__" and __package__ is None:
    from sys import path
    # noinspection PyShadowingBuiltins
    from os.path import dirname as dir
    path.append(dir(path[0]))
    __package__ = "SimpleSpigotHTTPTest"

from Downloaders.SpigotHTTP import SpigotHTTP


def main():
    # spigot = SpigotHTTP("hb1", 3, "E:\\Temp\\SpigotData")
    spigot = SpigotHTTP("hb3a", 123, "E:\\Temp\\SpigotData")
    # spigot = SpigotHTTP("hb3a", 588, "E:\\Temp\\SpigotData")
    # spigot = SpigotHTTP("hb3a", 566, "E:\\Temp\\SpigotData")
    spigot.download()

if __name__ == "__main__":
    main()
