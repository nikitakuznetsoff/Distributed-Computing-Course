from django.shortcuts import render
from django.shortcuts import render
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
from django.template.response import TemplateResponse
import secrets
import json
import subprocess
import os

@csrf_exempt
def simple_upload(request):
    if request.method == 'POST' and request.FILES['myfile']:
        myfile = request.FILES['myfile']
        if myfile.name.endswith('.txt'):
            myfile.name = secrets.token_hex(16) + ".txt"
            fs = FileSystemStorage()
            filename = fs.save(myfile.name, myfile)
            #uploaded_file_url = fs.url(filename)
            uploaded_file_url = "Media"
            write_to_file(myfile.name)
            return render(request, 'distrcomp/index.html', {
                'uploaded_file_url': uploaded_file_url
            })
    return render(request, 'distrcomp/index.html')

def write_to_file(name):
    text = read_from_file(name)
    f = open('spark-nltk.py', 'w')
    q = "import matplotlib.pyplot as plt\nplt.plot([1, 2, 3, 4])\nplt.ylabel('some numbers')\nplt.show()"
    f.write(q)
    f.close

def read_from_file(name):
    f = open('media/' + name, 'r')
    str = f.read()
    f.close()
    return str

def qwe(request):
    subprocess.call(['/usr/lib/spark/bin/spark-submit', '--master', 'spark://espero-pc:7077', 'media/spark-nltk.py'])

    filename = "report.py"
    f = open("media/ans_arg.txt", "r")
    js = f.read()
    f.close()
    str = json.loads(js)
    x = [str["python_time"], str["spark_time"]]
    content = "import matplotlib.pyplot as plt\nimport numpy as np\nx = np.arange(2)\nplt.bar([3,5], height={})\nplt.xticks([3,5], ['python_time','spark_time'], size=24)\nplt.suptitle('size = {}', size=24)\nplt.show()".format(x,str["file_size"])
    response = HttpResponse(content, content_type='text/plain')
    response['Content-Disposition'] = 'attachment; filename={0}'.format(filename)
    return response
