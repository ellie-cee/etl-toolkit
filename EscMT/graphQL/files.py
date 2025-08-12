from .base import *
from ..misc import *

class Files(GraphQL):
    def __init__(self, debug=False, searchable=True, minThrottle=1000):
        super().__init__(debug, searchable, minThrottle)
    def upload(self,url,altText=""):
        return self.run(
             """
            mutation fileCreate($files: [FileCreateInput!]!) {
                fileCreate(files: $files) {
                    files {
                        preview {
                            image {
                                url 
                            }
                        } 
                        fileStatus
                        fileErrors {
                            code
                            details
                            message
                        }
                        id
                    }
                    userErrors {
                        code
                        field
                        message
                    } 
                } 
            }""",
            {
                'files': [{
                    'alt': altText,
                    'contentType': 'IMAGE',
                    'originalSource': url,
                    'filename':url.split("?")[0].split("/")[-1],
                    'duplicateResolutionMode':"REPLACE"
                }]
            }
        )