from celery.task import task
from wand.image import Image
from wand.color import Color
from boto3 import client
from shutil import copyfile
import hashlib, textwrap, boto3, os,pathlib,tempfile
from PIL import Image as IG

def imageThumbnail(bucket,key,target_fname,width=100,height=100,force_exists=False):
    
    if key.endswith('Thumbs.db') or key.endswith('.DS_Store'):
        deleteObject(bucket,key)
        if os.path.exists(target_fname):
            os.remove(target_fname)
    elif key[-4:].lower() in ['.webm', '.mkv', '.flv', '.vob', '.ogv', '.ogg', '.rrc', '.gifv', '.mng', '.mov', '.avi', '.qt', '.wmv', '.yuv', '.rm', '.asf', '.amv', '.mp4', '.m4p', '.m4v', '.mpg', '.mp2', '.mpeg', '.mpe', '.mpv', '.m4v', '.svi', '.3gp', '.3g2', '.mxf', '.roq', '.nsv', '.flv', '.f4v', '.f4p', '.f4a', '.f4b']:
        src_default=os.path.join(pathlib.Path(__file__).parent.resolve(),"files/video.png")
        copyfile(src_default,target_fname)
        return target_fname
    elif key[-4:].lower() in ['.aac', '.aiff', '.ape', '.au', '.flac', '.gsm', '.it', '.m3u', '.m4a', '.mid', '.mod', '.mp3', '.mpa', '.pls', '.ra', '.s3m', '.sid', '.wav', '.wma', '.xm']:
        src_default=os.path.join(pathlib.Path(__file__).parent.resolve(),"files/audio.png")
        copyfile(src_default,target_fname)
        return target_fname
    elif key[-4:].lower() in ['.doc','.docx','.odt','.rft','.tex','.txt','.text','.wpd','.csv','.dat','.log','.mdb','.sql','.xml','.ods','.xls','.xlsx']:
        src_default=os.path.join(pathlib.Path(__file__).parent.resolve(),"files/default-thumbnail.png")
        copyfile(src_default,target_fname)
        return target_fname
    elif key[-4:].lower() in ['.tar','.zip','.pkg','.deb','.arj','.7z','.rar','.rpm','r.gz']:
        src_default=os.path.join(pathlib.Path(__file__).parent.resolve(),"files/zip.png")
        copyfile(src_default,target_fname)
        return target_fname
    elif force_exists==False and pathlib.Path(target_fname).is_file():
        return target_fname
    elif key[-4:].lower() in ['.pdf']:
        try:
            tmpfile= "{0}/{1}".format(tempfile.gettempdir(),next(tempfile._get_candidate_names()))
            object_content=genS3Objct(bucket,key)
            with open(tmpfile,'wb') as f:
                f.write(object_content.read())
            with Image(filename="{0}[0]".format(tmpfile)) as img:
                img.format = 'png'
                img.alpha_channel = 'remove'
                img.background_color = Color('white')
                img.thumbnail(width,height)
                img.save(filename=target_fname)
            return target_fname
        finally:
            if os.path.exists(tmpfile):
                print('delete: ',tmpfile)
                os.remove(tmpfile)
    else:
        try:
            object_content=genS3Objct(bucket,key)
            IG.MAX_IMAGE_PIXELS = None
            with IG.open(object_content) as img:
                new_img=img.convert('RGB')
                new_img.thumbnail((width,height))
                new_img.save(target_fname)
            print('worked')
            return target_fname
        except Exception as e:
            #try:
            #tmpfile= "{0}/{1}".format(tempfile.gettempdir(),next(tempfile._get_candidate_names()))
            object_content=genS3Objct(bucket,key)
            # with open(tmpfile,'wb') as f:
            #     f.write(object_content.read())
            with Image(blob=object_content.read()) as img:
                img.format = 'png'
                img.alpha_channel = 'remove'
                img.background_color = Color('white')
                img.thumbnail(width,height)
                img.save(filename=target_fname)
            print('Error')
            return target_fname
            # finally:
            #     if os.path.exists(tmpfile):
            #         print('delete: ',tmpfile)
            #         os.remove(tmpfile)
                


def genHash(key,split=7):
    r=hashlib.md5(key.encode())
    hashpath="/".join(textwrap.wrap(r.hexdigest(),split))
    return hashpath

def deleteObject(bucket,key):
    client = boto3.client('s3')
    client.delete_object(Bucket=bucket, Key=key)
    return True

def genS3Objct(bucket,key):
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(Bucket=bucket, Key=key)
    return s3_response_object['Body']

@task()
def generateObjectThumbnail(bucket,key,width=100,height=100,force_exists=False,target_base='/static_secure/thumbnails'):
    """
    Taskname: generateObjectThumbnail
    args/kwargs:bucket,width=100,height=100,target_base='/static_secure/thumbnails'
    """
    try:   
        hashkey="{0}/{1}".format(bucket,key)
        hashpath=genHash(hashkey)
        thumb_filename=os.path.join(target_base,hashpath,"thumbnail.png")
        pathlib.Path(os.path.join(target_base,hashpath)).mkdir(parents=True, exist_ok=True)
        imageThumbnail(bucket,key,thumb_filename,width,height,force_exists=force_exists)
        result={"key":key,"thumbnail":thumb_filename}
    except Exception as e:
        print(str(e))
        thumb_filename=os.path.join(target_base,hashpath,"thumbnail.png")
        src_default=os.path.join(pathlib.Path(__file__).parent.resolve(),"files/default-thumbnail.png")
        pathlib.Path(os.path.join(target_base,hashpath)).mkdir(parents=True, exist_ok=True)
        copyfile(src_default,thumb_filename)
        result={"key":key,"thumbnail":thumb_filename}
        pass
    return result

@task()
def generateBucketThumbnail(bucket,width=100,height=100,force_exists=False,target_base='/static_secure/thumbnails'):
    """
    Taskname: generateBucketThumbnail
    args/kwargs:bucket,width=100,height=100,target_base='/static_secure/thumbnails'
    """
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    result=[]
    for my_bucket_object in my_bucket.objects.all():
        result.append(generateObjectThumbnail(my_bucket_object.bucket_name,my_bucket_object.key,width=width,height=height,target_base=target_base,force_exists=force_exists))
    return result