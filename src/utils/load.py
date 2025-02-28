import json
import os
from typing import Optional, Dict

from strenum import StrEnum

class ExempleFilesEnum(StrEnum):
    EX = "exemple.json"
class filS3Parser:
    def __init__(self,path:str,id:str):
        self.path=path
        self.id=id
    def get_info(self)->Optional[Dict]:
        run_info_key =os.path.join(self.path,self.run +".json")
        with open(run_info_key) as f:
            run_info = json.load(f)
        if run_info["status"]== "ERROR":
            raise ValueError("run is not completed")
        return run_info