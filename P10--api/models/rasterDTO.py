from flask_restful import fields

class RasterDTO(object):
    def __init__(self, totalTrajectories, maxDraught, geometry):
        self.totalTrajectories = totalTrajectories
        self.maxDraught = maxDraught
        self.geometry = geometry

    def getDTO():
        return { 'totalTrajectories': fields.Integer, 'maxDraught': fields.Float, 'geometry': fields.String }