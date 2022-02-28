from flask_restful import fields

from models.rasterDTO import RasterDTO

class ViewDTO(object):
    def __init__(self, data):
        self.data = data

    def getDTO():
        return { 'data': fields.List(fields.Nested(RasterDTO.getDTO())) }