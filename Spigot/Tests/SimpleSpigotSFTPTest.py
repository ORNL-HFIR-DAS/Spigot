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
    __package__ = "SimpleSpigotSFTPTest"

from Downloaders.SpigotSFTP import SpigotSFTP


def main():
    spigot = SpigotSFTP("cg2", '0828', 206, "E:\\Temp\\SpigotData", "rgb")
    spigot.download()

if __name__ == "__main__":
    main()
