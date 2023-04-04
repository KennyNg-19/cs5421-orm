import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# DEFAULT_AUTO_FIELD='django.db.models.AutoField'
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'northwind2',
        'USER': 'root',
        'PASSWORD': '3.926puanY',
        'HOST': 'localhost',
        'PORT': '3306',
    }
}

MIDDLEWARE = [
    "debug_toolbar.middleware.DebugToolbarMiddleware",
]

INSTALLED_APPS = ["app", "debug_toolbar"]

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
    }
]

DEBUG_TOOLBAR_CONFIG = {
    # Django's test client sets wsgi.multiprocess to True inappropriately
    "RENDER_PANELS": False
}
