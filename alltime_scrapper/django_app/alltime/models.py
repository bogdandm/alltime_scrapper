from django.db import models

from const import DOMAIN


class CatalogWatch(models.Model):
    name = models.CharField(max_length=1024, verbose_name="Модель")
    href = models.URLField(max_length=1024, verbose_name="Ссылка")
    image_href = models.URLField(max_length=1024, verbose_name="Рис.")
    price = models.PositiveIntegerField("Цена")
    price_old = models.PositiveIntegerField("Цена 2", null=True)
    text = models.TextField(verbose_name="Текст")

    class Meta:
        verbose_name = "Модель в каталоге"
        verbose_name_plural = "Модели в каталоге"

    @property
    def absolute_image_href(self):
        return DOMAIN + self.image_href
