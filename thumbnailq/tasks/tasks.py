from celery.task import task
from wand.image import Image
from wand.color import Color
from boto3 import client
from shutil import copyfile
import hashlib, textwrap, boto3, os,pathlib,tempfile

def imageThumbnail(object_content,target_fname,width=100,height=100,blob=True):
    if blob:
        img=Image(blob=object_content)
    else:
        tmpfile= "{0}/file.try".format(tempfile.gettempdir())
        with open(tmpfile,'wb') as f:
            f.write(object_content)
        img = Image(filename="{0}[0]".format(tmpfile))
    img.format = 'png'
    img.alpha_channel = 'remove'
    img.background_color = Color('white')
    #img.transform(resize='320x240>')
    img.thumbnail(width,height)
    img.save(filename=target_fname)
    return target_fname

def genHash(key,split=7):
    r=hashlib.md5(key.encode())
    hashpath="/".join(textwrap.wrap(r.hexdigest(),split))
    return hashpath

def genS3Objct(bucket,key):
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(Bucket=bucket, Key=key)
    object_content = s3_response_object['Body'].read()
    return object_content

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
        object_content=genS3Objct(bucket,key)
        try:
            imageThumbnail(object_content,thumb_filename,width,height)
        except:
            imageThumbnail(object_content,thumb_filename,width,height,blob=False)
        result={"key":key,"thumbnail":thumb_filename}
    except Exception as e:
        thumb_filename=os.path.join(target_base,hashpath,"thumbnail.png")
        #src_default=os.path.join(pathlib.Path().resolve(),"thumbnailq/tasks","files/default-thumbnail.png")
        src_default=os.path.join(pathlib.Path(__file__).parent.resolve(),"files/default-thumbnail.png")
        pathlib.Path(os.path.join(target_base,hashpath)).mkdir(parents=True, exist_ok=True)
        copyfile(src_default,thumb_filename)
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