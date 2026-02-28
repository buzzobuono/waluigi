class Task:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        # Generiamo un ID univoco basato sui parametri
        self.param_str = "_".join(f"{k}-{v}" for k, v in sorted(kwargs.items()))
        self.id = f"{self.__class__.__name__}_{self.param_str}"

    def requires(self):
        return []

    def run(self):
        raise NotImplementedError
