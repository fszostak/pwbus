### PWBus - Filesystem Library
#:
#:  maintainer: fabio.szostak@perfweb.com.br | Sat Nov 16 16:36:36 -03 2019
 
import shutil

## rmdir_temp
#
def rmdir_temp(dir):
    if dir.startswith('temp'):
        return False
    try:
        shutil.rmtree(dir)
    except:
        return False

    return True

