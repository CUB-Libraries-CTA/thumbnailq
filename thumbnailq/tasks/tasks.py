from celery.task import task
from wand.image import Image
from wand.color import Color
from boto3 import client
from shutil import copyfile
import hashlib, textwrap, boto3, os,pathlib,tempfile
from PIL import Image as IG

def imageThumbnail(bucket,key,target_fname,width=100,height=100,blob=True):
    try:
        object_content=genS3Objct(bucket,key)
        IG.MAX_IMAGE_PIXELS = None
        with IG.open(object_content) as img:
            img.thumbnail((width,height))
            #print("1save")
            img.save(target_fname)
            print("1saveComplete")
        return target_fname
    except Exception as e:
        print("1save: ",str(e))
        pass
    try:
        object_content=genS3Objct(bucket,key)
        with Image(blob=object_content.read()) as img:
            img.format = 'png'
            img.alpha_channel = 'remove'
            img.background_color = Color('white')
            img.thumbnail(width,height)
            #print("2save")
            img.save(filename=target_fname)
            print("2saveComplete")
        return target_fname
    except Exception as e1:
        print("2save: ",str(e1))
        tmpfile= "{0}/file.try".format(tempfile.gettempdir())
        object_content=genS3Objct(bucket,key)
        with open(tmpfile,'wb') as f:
            f.write(object_content.read())
        with Image(filename="{0}[0]".format(tmpfile)) as img:
            img.format = 'png'
            img.alpha_channel = 'remove'
            img.background_color = Color('white')
            #img.transform(resize='320x240>')
            img.thumbnail(width,height)
            #print("3save")
            img.save(filename=target_fname)
            print("3saveComplete")
        return target_fname
    return target_fname

def genHash(key,split=7):
    r=hashlib.md5(key.encode())
    hashpath="/".join(textwrap.wrap(r.hexdigest(),split))
    return hashpath

def genS3Objct(bucket,key):
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(Bucket=bucket, Key=key)
    #object_content = s3_response_object['Body'].read()
    return s3_response_object['Body']
2018
@task()
def generateObjectThumbnail(bucket,key,width=100,height=100,target_base='/static_secure/thumbnails'):
    """
    Taskname: generateObjectThumbnail
    args/kwargs:bucket,width=100,height=100,target_base='/static_secure/thumbnails'
    """
    try:   
        hashkey="{0}/{1}".format(bucket,key)
        hashpath=genHash(hashkey)
        thumb_filename=os.path.join(target_base,hashpath,"thumbnail.png")
        pathlib.Path(os.path.join(target_base,hashpath)).mkdir(parents=True, exist_ok=True)
        #object_content=genS3Objct(bucket,key)
        #try:
        imageThumbnail(bucket,key,thumb_filename,width,height)
        #except:
        #    imageThumbnail(object_content,thumb_filename,width,height,blob=False)
        #object_content=None
        result={"key":key,"thumbnail":thumb_filename}
    except Exception as e:
        print("3save: ",str(e))
        thumb_filename=os.path.join(target_base,hashpath,"thumbnail.png")
        #src_default=os.path.join(pathlib.Path().resolve(),"thumbnailq/tasks","files/default-thumbnail.png")
        src_default=os.path.join(pathlib.Path(__file__).parent.resolve(),"files/default-thumbnail.png")
        pathlib.Path(os.path.join(target_base,hashpath)).mkdir(parents=True, exist_ok=True)
        copyfile(src_default,thumb_filename)
        print("4saveComplete")
        result={"key":key,"thumbnail":thumb_filename}
        pass
    return result

@task()
def generateBucketThumbnail(bucket,width=100,height=100,target_base='/static_secure/thumbnails'):
    """
    Taskname: generateBucketThumbnail
    args/kwargs:bucket,width=100,height=100,target_base='/static_secure/thumbnails'
    """
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    result=[]
    for my_bucket_object in my_bucket.objects.all():
        result.append(generateObjectThumbnail(my_bucket_object.bucket_name,my_bucket_object.key,width,height,target_base))
    return result