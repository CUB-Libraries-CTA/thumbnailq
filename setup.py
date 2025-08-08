from setuptools import setup, find_packages

setup(name='thumbnailq',
      version='0.1',
      packages= find_packages(),
      package_data={"thumbnailq": ['tasks/files/*.png', 'thumbnailq/tasks/files/*.png']},
      include_package_data=True,
      install_requires=[
          'celery==5.2.7',
          'pymongo==3.12.3',
          'Wand',
          'Pillow'
      ],
)
