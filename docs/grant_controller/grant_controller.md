# Grant Controller

The Moat grant controller allows policies defined using OPA and rego to be applied to databases types which do not 
directly support OPA. OPA is used as the decision engine, and the outcomes of the decisions are written to the target
database using its native grant system.

## How it works
The grant controller agent is triggered when a change is made to an OPA bundle. Bundle changes suggest that permissions 
in a target system need to be updated. When the grant controller is invoked, it follows this general process:

### User management
1. List principals which OPA is aware of
2. List principals in the target system
3. Compare results and create any users which do not exist

### User grant management
1. For each user: 
   1. Determine the tables / columns they have access to
   2. List grants for the user in target system
   3. Compare results and apply missing grants
   4. Revoke superfluous grants

## State Management
To enable changes to be quickly applies to target systems, an incremental change strategy has been employed.
The state of the target system is defined as the timestamp of the latest change to principal or object. This
allows the delta of changes to be applied to the target system for near-realtime permissions control. Semi-frequent 
full synchronisation is also recommended for completeness. 

## Postgres Example
For this example, we will be using the postgres database engine with simple ABAC policies defined in rego.

### Limitations
* Users are never dropped from the target database, as they can have permissions outside of the managed scope
* Scope is defined by the list of objects returned by the `TBA` query
* User scope is those known to Moat, any other users are untouched