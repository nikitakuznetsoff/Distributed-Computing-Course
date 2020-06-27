from django.urls import path
from . import views

urlpatterns = [
    path('', views.simple_upload),
    path('downLoad', views.qwe, name="script"),
]
