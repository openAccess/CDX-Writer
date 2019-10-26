# This is a minimal setup script intended just for building PEX.
# Generate cdx_writer.pex with:
#   pex -r requirements.txt -o cdx_writer.pex -m cdx_writer .
from setuptools import setup

setup(
    name='CDX-Writer',
    version='0.3.3',
    py_modules=['cdx_writer'],
    etras_require={
        'test': [
            'pytest<5;python_version<"3"',
            'pytest;python_version>="3"'
        ]
    }
)
