# Audit
Moat supports audit logging of changes to principals, resources and their respective attributes. This is supported using
two optional patterns:

## History Tables
Any change to an audit-relevant object (principal, group, resource or attribute) is logged in a durable history table in
the database. These tables can be used for reporting directly, or offloaded to a data lake or warehouse for cold storage

## Events / Webhooks
Events are webhooks are also available as a means of auditing, see [Monitoring, Events & Webhooks](../deployment/monitoring.md)