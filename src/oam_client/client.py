import asset_model

class Client:
    name: str
    
    def __init__(self, name: str):
        self.name = name
        self.asset = asset_model.FQDN("exemple.com")
    
