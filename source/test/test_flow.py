from waluigi.core.task import Task

class Step1(Task):
    def run(self):
        print("--- ESECUZIONE STEP 1 ---")
        # Qui potresti creare un file o fare una query

class MainTask(Task):
    def requires(self):
        # Il MainTask aspetta lo Step1
        return [Step1(params=vars(self.params))]
        
    def run(self):
        print("--- ESECUZIONE MAIN TASK ---")
