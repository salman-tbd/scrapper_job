"""
Django settings for australia_job_scraper project.
"""

from pathlib import Path
import os
from datetime import timedelta

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env if present
try:
    from dotenv import load_dotenv  # type: ignore

    # Prefer project root .env, then fallback to package dir .env, then default search
    env_candidates = [BASE_DIR.parent / ".env", BASE_DIR / ".env"]
    loaded = False
    for env_path in env_candidates:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            loaded = True
            break
    if not loaded:
        # Let python-dotenv search upward as a last resort
        load_dotenv()
except Exception:
    # Safe to ignore if python-dotenv isn't installed; env can still come from OS
    pass

# Security
SECRET_KEY = os.getenv("SECRET_KEY", "django-insecure-your-secret-key-here-change-in-production")
DEBUG = os.getenv("DEBUG", "1") in ["1", "true", "True"]

# Hosts / CSRF (set via env; safe defaults for local/dev)
# Example env in docker-compose:
#   ALLOWED_HOSTS=localhost,127.0.0.1,192.168.1.45
#   CSRF_TRUSTED_ORIGINS=http://192.168.1.45:8001
ALLOWED_HOSTS = [h.strip() for h in os.getenv("ALLOWED_HOSTS", "").split(",") if h.strip()]
# In development, allow all hosts to avoid DisallowedHost when testing via LAN IPs
if DEBUG and "*" not in ALLOWED_HOSTS:
    ALLOWED_HOSTS.append("*")
CSRF_TRUSTED_ORIGINS = [o.strip() for o in os.getenv("CSRF_TRUSTED_ORIGINS", "").split(",") if o.strip()]

# Apps
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",

    'rest_framework',
    'django_celery_beat',


    "corsheaders",  # enable CORS for cross-origin requests

    # Project apps
    "apps.core",
    "apps.companies",
    "apps.jobs",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",

    # CORS must be high in the stack
    "corsheaders.middleware.CorsMiddleware",

    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "australia_job_scraper.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "australia_job_scraper.wsgi.application"

# Database (uses env from docker-compose; falls back to SQLite if missing)
if os.getenv("DB_NAME"):
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.getenv("DB_NAME"),
            "USER": os.getenv("DB_USER", "postgres"),
            "PASSWORD": os.getenv("DB_PASSWORD", ""),
            "HOST": os.getenv("DB_HOST", "localhost"),
            "PORT": os.getenv("DB_PORT", "5432"),
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        }
    }

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# Internationalization
LANGUAGE_CODE = "en-us"
# Keep project local time for admin display
TIME_ZONE = 'Asia/Kolkata'
USE_I18N = True
# Store datetimes in UTC in DB; Django converts to TIME_ZONE for display
USE_TZ = True

# Static files
STATIC_URL = "static/"
STATIC_ROOT = os.getenv("STATIC_ROOT", str(BASE_DIR / "staticfiles"))
STATICFILES_DIRS = [BASE_DIR / "static"]

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Django REST Framework
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework_simplejwt.authentication.JWTAuthentication",
        "rest_framework.authentication.SessionAuthentication",  # enable DRF UI login in dev
    ],
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated",
    ],
    "DEFAULT_FILTER_BACKENDS": [
        "rest_framework.filters.SearchFilter",
        "rest_framework.filters.OrderingFilter",
    ],
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
        "rest_framework.renderers.BrowsableAPIRenderer",
    ],
    "DEFAULT_SCHEMA_CLASS": "rest_framework.schemas.coreapi.AutoSchema",
}

# SimpleJWT settings (can be overridden via env if needed)
SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(minutes=int(os.getenv("JWT_ACCESS_MINUTES", "60"))),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=int(os.getenv("JWT_REFRESH_DAYS", "7"))),
    "ROTATE_REFRESH_TOKENS": False,
    "BLACKLIST_AFTER_ROTATION": False,
    "ALGORITHM": os.getenv("JWT_ALGORITHM", "HS256"),
    "SIGNING_KEY": os.getenv("JWT_SIGNING_KEY", SECRET_KEY),
    # Accept common header types sent by various clients
    # e.g., "Authorization: Bearer <token>", "JWT <token>", or "Token <token>"
    "AUTH_HEADER_TYPES": ("Bearer", "bearer", "JWT", "jwt", "Token", "token"),
    # Ensure we read from the standard Django/WSGI auth header
    "AUTH_HEADER_NAME": "HTTP_AUTHORIZATION",
}

# CORS (open for dev; restrict in prod)
CORS_ALLOW_ALL_ORIGINS = os.getenv("CORS_ALLOW_ALL_ORIGINS", "1") in ["1", "true", "True"]
# If you want to restrict instead, set:
# CORS_ALLOWED_ORIGINS=http://192.168.1.45:8001,http://localhost:3000
if not CORS_ALLOW_ALL_ORIGINS:
    CORS_ALLOWED_ORIGINS = [
        o.strip() for o in os.getenv("CORS_ALLOWED_ORIGINS", "").split(",") if o.strip()
    ]


# Celery configuration
# Broker/result backend can be overridden via environment variables
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', CELERY_BROKER_URL)
CELERY_TIMEZONE = 'UTC'
CELERY_ENABLE_UTC = True
CELERY_TASK_ALWAYS_EAGER = False
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'
"""
Celery connection resilience
- Celery 6.0+ changes the startup retry behavior. To retain the existing
  behavior (retry connecting to the broker during startup), enable the
  following setting. Max retries -1 means retry forever.
"""
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_BROKER_CONNECTION_MAX_RETRIES = -1
