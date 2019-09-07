from django.contrib import admin

from .models import CatalogWatch


@admin.register(CatalogWatch)
class CatalogWatchAdmin(admin.ModelAdmin):
    list_display = ('name', 'price', 'image_tag',)