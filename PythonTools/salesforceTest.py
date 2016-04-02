__author__ = 'Brad'
from simple_salesforce import Salesforce
import json
sf = Salesforce( username='bbzylstra@randolphcollege.edu.careeco.dev', password='_moxie100', security_token='V6oPmGSaXW6Q5cv8MAEhLqCK', sandbox=True)
print sf

# Event = 'Care_Functional_Monitoring_Event__c'
# jsonData = json.dumps({'Patient__c': '003q0000003AizA', 'Metric__c': "Stepcount", "Severity__c": 1, 'Metric_Value__c': 0, "Message__c": "Stepcount is Zero"})
# print jsonData
# code = 'print sf.'+Event+'.create('+jsonData+')'
#
# exec code
# Event = 'Care_Functional_Monitoring_Event__c'
# jsonData = json.dumps({
#     "patientId": "351881065297355/11",
#     "event": {
#         "metric": "step-count",
#         "severity": 1,
#         "metricValue": 0,
#         "message": "Zero Stepcount"
#     }
# })
# code = 'print sf.'+Event+'.create('+jsonData+')'
# print jsonData
# exec code
#
# metric = 'Care_Functional_Monitoring_Metric__c'
# jsonData = json.dumps({'Patient__c': '003q0000003AizA', 'Metric__c': 'step-count', 'Metric_Value__c': 2,'Metric__c': 'gait-speed', 'Metric_Value__c': 1,\
#     'Metric__c': 'max-life-space', 'Metric_Value__c': 10,'Metric__c': 'in-home-transistions', 'Metric_Value__c': 5})
# print jsonData
#
# code = 'print sf.'+metric+'.create('+jsonData+')'
#
# exec code

data = {
    "patientId": "351881065297355/11", "event": {"metric": "step-count", "severity": 1, "metricValue": 0, "message": "Zero Stepcount"}}
print data
result = sf.apexecute('FMEvent/insertEvents', method='POST', data=data)
print result
metricData = {
    "patientId": "351881065297355/11",
    "metricList": [
        {
            "metric": "gait-speed",
            "metricValue": 0
        },
        {
            "metric": "step-count",
            "metricValue": 1400
        },
        {
            "metric": "max-life-space",
            "metricValue": 56
        },
        {
            "metric": "in-home-transitions",
            "metricValue": 100
        }
    ]
}
print metricData
result = sf.apexecute('FMMetrics/insertMetrics', method='POST', data=metricData)
print result