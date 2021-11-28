FROM culibraries/celery:0.0.4

RUN apk add imagemagick
ENV MAGICK_HOME /usr
RUN ln -s /usr/lib/libMagickCore-7.Q16HDRI.so.9 /usr/lib/libMagickCore-7.Q16HDRI.so
RUN ln -s /usr/lib/libMagickWand-7.Q16HDRI.so.9 /usr/lib/libMagickWand-7.Q16HDRI.so
RUN apk add jpeg-dev zlib-dev build-base 
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --upgrade Pillow