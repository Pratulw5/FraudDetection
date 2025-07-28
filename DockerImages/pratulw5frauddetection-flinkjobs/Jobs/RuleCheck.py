from pyflink.datastream.functions import KeyedProcessFunction
import json
class RuleCheck(KeyedProcessFunction):

    def process_element(self, value, ctx):
        txn = json.loads(value)
        # Cast all fields to float (or int where appropriate)
        old_org   = float(txn["oldbalanceOrg"])
        new_org   = float(txn["newbalanceOrig"])
        old_dest  = float(txn["oldbalanceDest"])
        new_dest  = float(txn["newbalanceDest"])
        amt       = float(txn["amount"])

        # Now your rule logic with real numbers
        origin_ok = (
            (old_org == 0 and new_org == 0)
            or (old_org < amt and new_org == 0)
            or (new_org == old_org - amt)
        )
        dest_ok = (new_dest == old_dest + amt)

        txn["Rules_Check"] = int(not (origin_ok and dest_ok))
        yield json.dumps(txn)
