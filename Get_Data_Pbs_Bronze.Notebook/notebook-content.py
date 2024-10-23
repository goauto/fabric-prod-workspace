# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "76325447-8576-4ab5-a58b-6dd9319e8be7",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "cf6eae1e-540c-45e8-b1bd-ed425d395cfd"
# META     }
# META   }
# META }

# CELL ********************

%run /api_base_class

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from pyspark.sql.types import *
import pyodbc
from pyspark.sql.functions import current_timestamp, from_utc_timestamp
from delta.tables import DeltaTable

from datetime import datetime, timedelta

import pytz

import time

# KEY_VAULT_URI
KEY_VAULT_URI = 'https://CDKVault.vault.azure.net/'

#Fetch the secrets from key vault
username = mssparkutils.credentials.getSecret(KEY_VAULT_URI,"pbsusername")
password = mssparkutils.credentials.getSecret(KEY_VAULT_URI,"pbspassword")

#PbsApi Class which inherits the properties of ApiBaseClass Notebook
class PbsApi(ApiBaseClass):

    """
    PbsApi class pulls the data from PBS API
    """
    def __init__(self, username, password):
        """
        Constructor
        """
        super().__init__(username, password)  
        self.log = logging.getLogger(self.__class__.__name__)
        self.username = username
        self.password = password
        self.headers = {
                        'Content-Type': 'application/json',
                    }
        self.record_insert_timestamp = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Mountain'))

    #Function to make an API call
    def make_post_api_call(self, url, data):
        # Make the POST request to the API with HTTP Basic Authentication
        pbsapi.log.info(f'Making API call to {url} for serial number: {data.get("SerialNumber")}')
        response = requests.post(
        url,
        headers=self.headers,
        data=json.dumps(data),
        auth=HTTPBasicAuth(self.username, self.password)
        )
        if response.status_code == 200:
            response = response.json()
            return response
        else:
            pbsapi.log.info(f"Failed to get data for {url} for serial number {data.get('SerialNumber')}. Status code: {response.status_code}")

    #Function to parse Opcode Data
    def parse_opcode_response(self, response):
        opcode_data = []
        # Append the response data to the opcode_data list
        for opcode in response.get('OpCodes', []):
            opcode_data.append({
                'Id': opcode.get('Id', None),
                'SerialNumber': opcode.get('SerialNumber', None),
                'OpCodeId': opcode.get('OpCodeId', None),
                'Code': opcode.get('Code', None),
                'Description': opcode.get('Description', None),
                'FlatDollars': opcode.get('FlatDollars', None),
                'FlatHours': opcode.get('FlatHours', None),
                'AllowedHours': opcode.get('AllowedHours', None),
                'LabourEstimate': opcode.get('LabourEstimate', None),
                'PartsEstimate': opcode.get('PartsEstimate', None),
                'SkillCode': opcode.get('SkillCode', None),
                'RateCode': opcode.get('RateCode', None),
                'LastUpdate': opcode.get('LastUpdate', None),
                'IsWeb': opcode.get('IsWeb', None),
                'WebSequence': opcode.get('WebSequence', None),
                'GMCode': opcode.get('GMCode', None),
                'IsGMOpCode': opcode.get('IsGMOpCode', None),
                'IsDeleted': opcode.get('IsDeleted', None),
                'DisplayInPulldown': opcode.get('DisplayInPulldown', None),
                'Category': opcode.get('Category', None),
                'OpCodePriceCodes': opcode.get('OpCodePriceCodes', []),
                'PartLines': opcode.get('PartLines', []),
                'RecordInsertTimestamp': pbsapi.record_insert_timestamp
            })
        return opcode_data

    #Function to parse Contacts Data
    def parse_contacts_response(self, response):
        contacts_data = []
        # Append the response data to the contacts_data list
        for contact in response.get('Contacts', []):
            contacts_data.append({
                'Id': contact.get('Id', None),
                'ContactId': contact.get('ContactId', None),
                'SerialNumber': contact.get('SerialNumber', None),
                'Code': contact.get('Code', None),
                'LastName': contact.get('LastName', None),
                'FirstName': contact.get('FirstName', None),
                'Salutation': contact.get('Salutation', None),
                'MiddleName': contact.get('MiddleName', None),
                'ContactName': contact.get('ContactName', None),
                'IsInactive': contact.get('IsInactive', None),
                'IsBusiness': contact.get('IsBusiness', None),
                'ApartmentNumber': contact.get('ApartmentNumber', None),
                'Address': contact.get('Address', None),
                'City': contact.get('City', None),
                'County': contact.get('County', None),
                'State': contact.get('State', None),
                'ZipCode': contact.get('ZipCode', None),
                'BusinessPhone': contact.get('BusinessPhone', None),
                'BusinessPhoneExt': contact.get('BusinessPhoneExt', None),
                'HomePhone': contact.get('HomePhone', None),
                'CellPhone': contact.get('CellPhone', None),
                'BusinessPhoneRawReverse': contact.get('BusinessPhoneRawReverse', None),
                'HomePhoneRawReverse': contact.get('HomePhoneRawReverse', None),
                'CellPhoneRawReverse': contact.get('CellPhoneRawReverse', None),
                'FaxNumber': contact.get('FaxNumber', None),
                'EmailAddress': contact.get('EmailAddress', None),
                'Notes': contact.get('Notes', None),
                'CriticalMemo': contact.get('CriticalMemo', None),
                'BirthDate': contact.get('BirthDate', None),
                'Gender': contact.get('Gender', None),
                'DriverLicense': contact.get('DriverLicense', None),
                'DriversLicenseExpiry': contact.get('DriversLicenseExpiry', None),
                'PreferredContactMethods': contact.get('PreferredContactMethods', []),
                'LastUpdate': contact.get('LastUpdate', None),
                'CustomFields': contact.get('CustomFields', []),
                'FleetType': contact.get('FleetType', None),
                'RelationshipType': contact.get('RelationshipType', None),
                'CommunicationPreferencesEmail': contact.get('CommunicationPreferences', {}).get('Email', None),
                'CommunicationPreferencesPhone': contact.get('CommunicationPreferences', {}).get('Phone', None),
                'CommunicationPreferencesTextMessage': contact.get('CommunicationPreferences', {}).get('TextMessage', None),
                'CommunicationPreferencesLetter': contact.get('CommunicationPreferences', {}).get('Letter', None),
                'CommunicationPreferencesPreferred': contact.get('CommunicationPreferences', {}).get('Preferred', None),
                'CommunicationPreferencesFollowUp': contact.get('CommunicationPreferences', {}).get('FollowUp', None),
                'CommunicationPreferencesMarketing': contact.get('CommunicationPreferences', {}).get('Marketing', None),
                'CommunicationPreferencesThirdParty': contact.get('CommunicationPreferences', {}).get('ThirdParty', None),
                'CommunicationPreferencesImplicitConsentDate': contact.get('CommunicationPreferences', {}).get('ImplicitConsentDate', None),
                'SalesRepRef': contact.get('SalesRepRef', None),
                'Language': contact.get('Language', None),
                'PayableAccount': contact.get('PayableAccount', None),
                'ReceivableAccount': contact.get('ReceivableAccount', None),
                'IsStatic': contact.get('IsStatic', None),
                'PrimaryImageRef': contact.get('PrimaryImageRef', None),
                'PayableAccounts': contact.get('PayableAccounts',[]),
                'ReceivableAccounts': contact.get('ReceivableAccounts',[]),
                'IsAPVendor': contact.get('IsAPVendor', None),
                'IsARCustomer': contact.get('IsARCustomer', None),
                'ManufacturerLoyaltyNumber': contact.get('ManufacturerLoyaltyNumber', None),
                'MergedToContactRef': contact.get('MergedToContactRef', None),
                'DoNotLoadHistory': contact.get('DoNotLoadHistory', None),
                'RepairOrderRequiresPO': contact.get('RepairOrderRequiresPO', None),
                'RecordInsertTimestamp': pbsapi.record_insert_timestamp
            })
        return contacts_data

    #Function to parse Appointments Data
    def parse_appointments_response(self, response):
        appointments_data = []
        # Append the response data to the appointments_data list
        for appointment in response.get('Appointments', []):
            appointments_data.append({
                'Id': appointment.get('Id', None),
                'AppointmentId': appointment.get('AppointmentId', None),
                'SerialNumber': appointment.get('SerialNumber', None),
                'AppointmentNumber': appointment.get('AppointmentNumber', None),
                'RawAppointmentNumber': appointment.get('RawAppointmentNumber', None),
                'Shop': appointment.get('Shop', None),
                'Advisor': appointment.get('Advisor', None),
                'AdvisorRef': appointment.get('AdvisorRef', None),
                'BookingUser': appointment.get('BookingUser', None),
                'BookingUserRef': appointment.get('BookingUserRef', None),
                'Transportation': appointment.get('Transportation', None),
                'ContactRef': appointment.get('ContactRef', None),
                'VehicleRef': appointment.get('VehicleRef', None),
                'MileageIn': appointment.get('MileageIn', None),
                'IsComeback': appointment.get('IsComeback', None),
                'IsWaiter': appointment.get('IsWaiter', None),
                'AppointmentTime': appointment.get('AppointmentTime', None),
                'AppointmentTimeUTC': appointment.get('AppointmentTimeUTC', None),
                'PickupTime': appointment.get('PickupTime', None),
                'PickupTimeUTC': appointment.get('PickupTimeUTC', None),
                'RequestLines': appointment.get('RequestLines', []),
                'DateOpened': appointment.get('DateOpened', None),
                'LastUpdate': appointment.get('LastUpdate', None),
                'Status': appointment.get('Status', None),
                'Notes': appointment.get('Notes', None),
                'Source': appointment.get('Source', None),
                'PendingRequest': appointment.get('PendingRequest', None),
                'CheckedIn': appointment.get('CheckedIn', None),
                'Confirmed': appointment.get('Confirmed', None),
                'LeadRef': appointment.get('LeadRef', None),
                'NotifyType': appointment.get('NotifyType', None),
                'Tag': appointment.get('Tag', None),
                'FirstApptTimeAvailable': appointment.get('FirstApptTimeAvailable', None),
                'RecordInsertTimestamp': pbsapi.record_insert_timestamp
            })
        return appointments_data

    #Function to parse Repair Orders Data
    def parse_repairorders_response(self, response):
        repairorders_data = []
        # Append the response data to the repairorders_data list
        for order in response.get('RepairOrders', []):
            repairorders_data.append({
                'Id': order.get('Id',None),
                'RepairOrderId': order.get('RepairOrderId',None),
                'SerialNumber': order.get('SerialNumber',None),
                'RepairOrderNumber': order.get('RepairOrderNumber',None),
                'RawRepairOrderNumber': order.get('RawRepairOrderNumber',None),
                'DateOpened': order.get('DateOpened',None),
                'DateOpenedUTC': order.get('DateOpenedUTC',None),
                'DateCashiered': order.get('DateCashiered',None),
                'DateCashieredUTC': order.get('DateCashieredUTC',None),
                'DatePromised': order.get('DatePromised',None),
                'DatePromisedUTC': order.get('DatePromisedUTC',None),
                'DateVehicleCompleted': order.get('DateVehicleCompleted',None),
                'DateCustomerNotified': order.get('DateCustomerNotified',None),
                'CSR': order.get('CSR',None),
                'CSRRef': order.get('CSRRef',None),
                'BookingUser': order.get('BookingUser',None),
                'BookingUserRef': order.get('BookingUserRef',None),
                'ContactRef': order.get('ContactRef',None),
                'VehicleRef': order.get('VehicleRef',None),
                'MileageIn': order.get('MileageIn',None),
                'MileageOut': order.get('MileageOut',None),
                'Tag': order.get('Tag',None),
                'Location': order.get('Location',None),
                'IsWaiter': order.get('IsWaiter',None),
                'IsComeback': order.get('IsComeback',None),
                'Shop': order.get('Shop',None),
                'ChargeType': order.get('ChargeType',None),
                'PurchaseOrderNumber': order.get('PurchaseOrderNumber',None),
                'Transportation': order.get('Transportation',None),
                'Status': order.get('Status',None),
                'Memo': order.get('Memo', None),
                'MemoCustomerCopy': order.get('MemoCustomerCopy', None),
                'AppointmentNumber': order.get('AppointmentNumber', None),
                'AppointmentRef': order.get('AppointmentRef', None),
                'LastUpdate': order.get('LastUpdate', None),
                'IsHardCopyPrinted': order.get('IsHardCopyPrinted', None),
                'Priority': order.get('Priority', None),
                'TodayPhoneNumber': order.get('TodayPhoneNumber', None),
                'NotifyType': order.get('NotifyType', None),
                'IncludeInternalPricing': order.get('IncludeInternalPricing', None),
                'VINInquiryPerformed': order.get('VINInquiryPerformed', None),
                'SONote': order.get('SONote', None),
                'Requests': order.get('Requests',[]),
                'CustomerSummaryLabour': order.get('CustomerSummary',{}).get('Labour',None),
                'CustomerSummaryParts': order.get('CustomerSummary',{}).get('Parts',None),
                'CustomerSummaryOilGas': order.get('CustomerSummary',{}).get('OilGas',None),
                'CustomerSummarySubletTow': order.get('CustomerSummary',{}).get('SubletTow',None),
                'CustomerSummaryMisc': order.get('CustomerSummary',{}).get('Misc',None),
                'CustomerSummaryEnvironment': order.get('CustomerSummary',{}).get('Environment',None),
                'CustomerSummaryShopSupplies': order.get('CustomerSummary',{}).get('ShopSupplies',None),
                'CustomerSummaryFreight': order.get('CustomerSummary',{}).get('Freight',None),
                'CustomerSummaryWarrantyDeductible': order.get('CustomerSummary',{}).get('WarrantyDeductible',None),
                'CustomerSummaryDiscount': order.get('CustomerSummary',{}).get('Discount',None),
                'CustomerSummarySubTotal': order.get('CustomerSummary',{}).get('SubTotal',None),
                'CustomerSummaryTax1': order.get('CustomerSummary',{}).get('Tax1',None),
                'CustomerSummaryTax2': order.get('CustomerSummary',{}).get('Tax2',None),
                'CustomerSummaryInvoiceTotal': order.get('CustomerSummary',{}).get('InvoiceTotal',None),
                'CustomerSummaryCustomerDeductible': order.get('CustomerSummary',{}).get('CustomerDeductible',None),
                'CustomerSummaryCustomerDeductibleBillableDescription': order.get('CustomerSummary',{}).get('CustomerDeductibleBillableDescription',None),
                'CustomerSummaryGrandTotal': order.get('CustomerSummary',{}).get('GrandTotal',None),
                'CustomerSummaryStatus': order.get('CustomerSummary',{}).get('Status',None),
                'CustomerSummaryDateCashiered': order.get('CustomerSummary',{}).get('DateCashiered',None),
                'CustomerSummaryLabourDiscount': order.get('CustomerSummary',{}).get('LabourDiscount',None),
                'CustomerSummaryPartDiscount': order.get('CustomerSummary',{}).get('PartDiscount',None),
                'CustomerSummaryServiceFeeTotal': order.get('CustomerSummary',{}).get('ServiceFeeTotal',None),
                'WarrantySummaryLabour': order.get('WarrantySummary',{}).get('Labour',None),
                'WarrantySummaryParts': order.get('WarrantySummary', {}).get('Parts', None),
                'WarrantySummaryOilGas': order.get('WarrantySummary', {}).get('OilGas', None),
                'WarrantySummarySubletTow': order.get('WarrantySummary', {}).get('SubletTow', None),
                'WarrantySummaryMisc': order.get('WarrantySummary', {}).get('Misc', None),
                'WarrantySummaryEnvironment': order.get('WarrantySummary', {}).get('Environment', None),
                'WarrantySummaryShopSupplies': order.get('WarrantySummary', {}).get('ShopSupplies', None),
                'WarrantySummaryFreight': order.get('WarrantySummary', {}).get('Freight', None),
                'WarrantySummaryWarrantyDeductible': order.get('WarrantySummary', {}).get('WarrantyDeductible', None),
                'WarrantySummaryDiscount': order.get('WarrantySummary', {}).get('Discount', None),
                'WarrantySummarySubTotal': order.get('WarrantySummary', {}).get('SubTotal', None),
                'WarrantySummaryTax1': order.get('WarrantySummary', {}).get('Tax1', None),
                'WarrantySummaryTax2': order.get('WarrantySummary', {}).get('Tax2', None),
                'WarrantySummaryInvoiceTotal': order.get('WarrantySummary', {}).get('InvoiceTotal', None),
                'WarrantySummaryCustomerDeductible': order.get('WarrantySummary', {}).get('CustomerDeductible', None),
                'WarrantySummaryCustomerDeductibleBillableDescription': order.get('WarrantySummary', {}).get('CustomerDeductibleBillableDescription', None),
                'WarrantySummaryGrandTotal': order.get('WarrantySummary', {}).get('GrandTotal', None),
                'WarrantySummaryStatus': order.get('WarrantySummary', {}).get('Status', None),
                'WarrantySummaryDateCashiered': order.get('WarrantySummary', {}).get('DateCashiered', None),
                'WarrantySummaryLabourDiscount': order.get('WarrantySummary', {}).get('LabourDiscount', None),
                'WarrantySummaryPartDiscount': order.get('WarrantySummary', {}).get('PartDiscount', None),
                'WarrantySummaryServiceFeeTotal': order.get('WarrantySummary', {}).get('ServiceFeeTotal', None),
                'InternalSummaryLabour': order.get('InternalSummary', {}).get('Labour', None),
                'InternalSummaryParts': order.get('InternalSummary', {}).get('Parts', None),
                'InternalSummaryOilGas': order.get('InternalSummary', {}).get('OilGas', None),
                'InternalSummarySubletTow': order.get('InternalSummary', {}).get('SubletTow', None),
                'InternalSummaryMisc': order.get('InternalSummary', {}).get('Misc', None),
                'InternalSummaryEnvironment': order.get('InternalSummary', {}).get('Environment', None),
                'InternalSummaryShopSupplies': order.get('InternalSummary', {}).get('ShopSupplies', None),
                'InternalSummaryFreight': order.get('InternalSummary', {}).get('Freight', None),
                'InternalSummaryWarrantyDeductible': order.get('InternalSummary', {}).get('WarrantyDeductible', None),
                'InternalSummaryDiscount': order.get('InternalSummary', {}).get('Discount', None),
                'InternalSummarySubTotal': order.get('InternalSummary', {}).get('SubTotal', None),
                'InternalSummaryTax1': order.get('InternalSummary', {}).get('Tax1', None),
                'InternalSummaryTax2': order.get('InternalSummary', {}).get('Tax2', None),
                'InternalSummaryInvoiceTotal': order.get('InternalSummary', {}).get('InvoiceTotal', None),
                'InternalSummaryCustomerDeductible': order.get('InternalSummary', {}).get('CustomerDeductible', None),
                'InternalSummaryCustomerDeductibleBillableDescription': order.get('InternalSummary', {}).get('CustomerDeductibleBillableDescription', None),
                'InternalSummaryGrandTotal': order.get('InternalSummary', {}).get('GrandTotal', None),
                'InternalSummaryStatus': order.get('InternalSummary', {}).get('Status', None),
                'InternalSummaryDateCashiered': order.get('InternalSummary', {}).get('DateCashiered', None),
                'InternalSummaryLabourDiscount': order.get('InternalSummary', {}).get('LabourDiscount', None),
                'InternalSummaryPartDiscount': order.get('InternalSummary', {}).get('PartDiscount', None),
                'InternalSummaryServiceFeeTotal': order.get('InternalSummary', {}).get('ServiceFeeTotal', None),
                'LoanerVehicleRef': order.get('Loaner', {}).get('VehicleRef', None),
                'LoanerFriendlyId': order.get('Loaner', {}).get('FriendlyId', None),
                'LoanerDatePickup': order.get('Loaner', {}).get('DatePickup', None),
                'LoanerDateDropOff': order.get('Loaner', {}).get('DateDropOff', None),
                'LoanerOdomPickup': order.get('Loaner', {}).get('OdomPickup', None),
                'LoanerOdomDropOff': order.get('Loaner', {}).get('OdomDropOff', None),
                'LoanerAgreementNumber': order.get('Loaner', {}).get('AgreementNumber', None),
                'LoanerComments': order.get('Loaner', {}).get('Comments', None),
                'PendingRequests': order.get('PendingRequests',[]),
                'DeferredRequests': order.get('DeferredRequests',[]),
                'CancelledRequests': order.get('CancelledRequests',[]),
                'RecordInsertTimestamp': pbsapi.record_insert_timestamp
            })
        return repairorders_data    

    #Function to parse Vehcle Inventory
    def parse_vehicle_response(self, response):
        vehicles_data = []
        # Append the response data to the opcode_data list
        for vehicle in response.get('Vehicles', []):
            vehicles_data.append({
                'Id': vehicle.get('Id', None),
                'VehicleId': vehicle.get('VehicleId', None),
                'SerialNumber': vehicle.get('SerialNumber', None),
                'StockNumber': vehicle.get('StockNumber', None),
                'VIN': vehicle.get('VIN', None),
                'LicenseNumber': vehicle.get('LicenseNumber', None),
                'FleetNumber': vehicle.get('FleetNumber', None),
                'Status': vehicle.get('Status', None),
                'OwnerRef': vehicle.get('OwnerRef', None),
                'ModelNumber': vehicle.get('ModelNumber', None),
                'Make': vehicle.get('Make', None),
                'Model': vehicle.get('Model', None),
                'Trim': vehicle.get('Trim', None),
                'VehicleType': vehicle.get('VehicleType', None),
                'Year': vehicle.get('Year', None),
                'Odometer': vehicle.get('Odometer', None),
                'ExteriorColorCode': vehicle.get('ExteriorColor', {}).get('Code', None),
                'ExteriorColorDescription': vehicle.get('ExteriorColor', {}).get('Description', None),
                'InteriorColorCode': vehicle.get('InteriorColor', {}).get('Code', None),
                'InteriorColorDescription': vehicle.get('InteriorColor', {}).get('Description', None),
                'Engine': vehicle.get('Engine', None),
                'Cylinders': vehicle.get('Cylinders', None),
                'Transmission': vehicle.get('Transmission', None),
                'DriveWheel': vehicle.get('DriveWheel', None),
                'Fuel': vehicle.get('Fuel', None),
                'Weight': vehicle.get('Weight', None),
                'InServiceDate': vehicle.get('InServiceDate', None),
                'LastServiceDate': vehicle.get('LastServiceDate', None),
                'LastServiceMileage': vehicle.get('LastServiceMileage', None),
                'Lot': vehicle.get('Lot', None),
                'LotDescription': vehicle.get('LotDescription', None),
                'Category': vehicle.get('Category', None),
                'Options': vehicle.get('Options', []),
                'Refurbishments': vehicle.get('Refurbishments', []),
                'OrderInvoiceNumber': vehicle.get('Order', {}).get('InvoiceNumber', None),
                'OrderPrice': vehicle.get('Order', {}).get('Price', None),
                'OrderStatus': vehicle.get('Order', {}).get('Status', None),
                'OrderEta': vehicle.get('Order', {}).get('Eta', None),
                'OrderDate': vehicle.get('Order', {}).get('OrderDate', None),
                'OrderEstimatedCost': vehicle.get('Order', {}).get('EstimatedCost', None),
                'OrderStatusDate': vehicle.get('Order', {}).get('StatusDate', None),
                'OrderIgnitionKeyCode': vehicle.get('Order', {}).get('IgnitionKeyCode', None),
                'OrderDoorKeyCode': vehicle.get('Order', {}).get('DoorKeyCode', None),
                'OrderDescription': vehicle.get('Order', {}).get('Description', None),
                'OrderLocationStatus': vehicle.get('Order', {}).get('LocationStatus', None),
                'OrderLocationStatusDate': vehicle.get('Order', {}).get('LocationStatusDate', None),
                'MSR': vehicle.get('MSR', None),
                'BaseMSR': vehicle.get('BaseMSR', None),
                'Retail': vehicle.get('Retail', None),
                'DateReceived': vehicle.get('DateReceived', None),
                'InternetPrice': vehicle.get('InternetPrice', None),
                'Lotpack': vehicle.get('Lotpack', None),
                'Holdback': vehicle.get('Holdback', None),
                'InternetNotes': vehicle.get('InternetNotes', None),
                'Notes': vehicle.get('Notes', None),
                'CriticalMemo': vehicle.get('CriticalMemo', None),
                'IsCertified': vehicle.get('IsCertified', None),
                'LastSaleDate': vehicle.get('LastSaleDate', None),
                'LastUpdate': vehicle.get('LastUpdate', None),
                'AppraisedValue': vehicle.get('AppraisedValue', None),
                'Warranties': vehicle.get('Warranties', []),
                'Freight': vehicle.get('Freight', None),
                'Air': vehicle.get('Air', None),
                'Inventory': vehicle.get('Inventory', None),
                'IsInactive': vehicle.get('IsInactive', None),
                'FloorPlanCode': vehicle.get('FloorPlanCode', None),
                'FloorPlanAmount': vehicle.get('FloorPlanAmount', None),
                'InsuranceCompany': vehicle.get('Insurance', {}).get('Company', None),
                'InsurancePolicy': vehicle.get('Insurance', {}).get('Policy', None),
                'InsuranceExpiryDate': vehicle.get('Insurance', {}).get('ExpiryDate', None),
                'InsuranceAgentName': vehicle.get('Insurance', {}).get('AgentName', None),
                'InsuranceAgentPhoneNumber': vehicle.get('Insurance', {}).get('AgentPhoneNumber', None),
                'Body': vehicle.get('Body', None),
                'ShortVIN': vehicle.get('ShortVIN', None),
                'AdditionalDrivers': vehicle.get('AdditionalDrivers', []),
                'OrderDetailsDistributor': vehicle.get('OrderDetails', {}).get('Distributor', None),
                'PrimaryImageRef': vehicle.get('PrimaryImageRef', None),
                'HoldVehicleRef': vehicle.get('Hold', {}).get('VehicleRef', None),
                'HoldFrom': vehicle.get('Hold', {}).get('HoldFrom', None),
                'HoldUntil': vehicle.get('Hold', {}).get('HoldUntil', None),
                'HoldUserRef': vehicle.get('Hold', {}).get('UserRef', None),
                'HoldContactRef': vehicle.get('Hold', {}).get('ContactRef', None),
                'HoldComments': vehicle.get('Hold', {}).get('Comments', None),
                'SeatingCapacity': vehicle.get('SeatingCapacity', None),
                'DeliveryDate': vehicle.get('DeliveryDate', None),
                'WarrantyExpiry': vehicle.get('WarrantyExpiry', None),
                'IsConditionallySold': vehicle.get('IsConditionallySold', None),
                'SalesDivision': vehicle.get('SalesDivision', None),
                'StyleRef': vehicle.get('StyleRef', None),
                'TotalCost': vehicle.get('TotalCost', None),
                'BlueBookValue': vehicle.get('BlueBookValue', None),
                'VideoURL': vehicle.get('VideoURL', None),
                'ShowOnWeb': vehicle.get('ShowOnWeb', None),
                'PDI': vehicle.get('PDI', None),
                'Configuration': vehicle.get('Configuration', None),
                'DisplayTrim': vehicle.get('DisplayTrim', None),
                'RecordInsertTimestamp': pbsapi.record_insert_timestamp
            })
        return vehicles_data

    #Function to parse Deals Data
    def parse_dealget_response(self, response):
        deals_data = []
        # Append the response data to the deals_data list
        for deal in response.get('Deals', []):
            deals_data.append({
                    'Id': deal.get('Id', None),
                    'DealId': deal.get('DealId', None),
                    'SerialNumber': deal.get('SerialNumber', None),
                    'DealKey': deal.get('DealKey', None),
                    'DealType': deal.get('DealType', None),
                    'UserRoles': deal.get('UserRoles', []),
                    'CreationDate': deal.get('CreationDate', None),
                    'ContractDate': deal.get('ContractDate', None),
                    'PaymentDate': deal.get('PaymentDate', None),
                    'DeliveryDate': deal.get('DeliveryDate', None),
                    'SystemDeliveryDate': deal.get('SystemDeliveryDate', None),
                    'SoldDate': deal.get('SoldDate', None),
                    'DeliveryStepsCompleted': deal.get('DeliveryStepsCompleted', []),
                    'Conditions': deal.get('Conditions', None),
                    'Status': deal.get('Status', None),
                    'SaleType': deal.get('SaleType', None),
                    'TaxCode': deal.get('TaxCode', None),
                    'Notes': deal.get('Notes', None),
                    'AmortizationTerm': deal.get('AmortizationTerm', None),
                    'PaymentTerm': deal.get('PaymentTerm', None),
                    'PaymentTermMonths': deal.get('PaymentTermMonths', None),
                    'PaymentsPerYear': deal.get('PaymentsPerYear', None),
                    'Price': deal.get('Price', None),
                    'BuyerRef': deal.get('BuyerRef', None),
                    'CoBuyerRefs': deal.get('CoBuyerRefs', []),
                    'LastUpdate': deal.get('LastUpdate', None),
                    'Fees': deal.get('Fees', []),
                    'Accessories': deal.get('Accessories', []),
                    'Warranties': deal.get('Warranties', []),
                    'Protections': deal.get('Protections', []),
                    'Insurance': deal.get('Insurance', []),
                    'Trades': deal.get('Trades', []),
                    'Rebates': deal.get('Rebates', []),
                    'Allowances': deal.get('Allowances', []),
                    'BackEndAllowances': deal.get('BackEndAllowances', []),
                    'Adjustments': deal.get('Adjustments', []),
                    'Vehicles': deal.get('Vehicles', []),
                    'VehicleInsuranceAgent': deal.get('VehicleInsurance',{}).get('Agent',None),
                    'VehicleInsuranceAddress': deal.get('VehicleInsurance',{}).get('Address',None),
                    'VehicleInsuranceCity': deal.get('VehicleInsurance',{}).get('City',None),
                    'VehicleInsuranceProvince': deal.get('VehicleInsurance',{}).get('Province',None),
                    'VehicleInsurancePostalCode': deal.get('VehicleInsurance',{}).get('PostalCode',None),
                    'VehicleInsurancePhone': deal.get('VehicleInsurance',{}).get('Phone',None),
                    'VehicleInsuranceFax': deal.get('VehicleInsurance',{}).get('Fax',None),
                    'VehicleInsuranceCompany': deal.get('VehicleInsurance',{}).get('InsuranceCompany',None),
                    'VehicleInsuranceSaleNumber': deal.get('VehicleInsurance',{}).get('AgentSaleNumber',None),
                    'VehicleInsurancePolicyNumber': deal.get('VehicleInsurance',{}).get('PolicyNumber',None),
                    'VehicleInsurancePolicyEffective': deal.get('VehicleInsurance',{}).get('PolicyEffective',None),
                    'VehicleInsurancePolicyExpiry': deal.get('VehicleInsurance',{}).get('PolicyExpiry',None),
                    'VehicleInsuranceCollision': deal.get('VehicleInsurance',{}).get('Collision',None),
                    'VehicleInsuranceComprehensive': deal.get('VehicleInsurance',{}).get('Comprehensive',None),
                    'VehicleInsuranceLiability': deal.get('VehicleInsurance',{}).get('Liability',None),
                    'CashInfoMSRP': deal.get('CashInfo',{}).get('MSRP',None),
                    'CashInfoTaxes': deal.get('CashInfo',{}).get('Taxes',[]),
                    'CashInfoDeposit': deal.get('CashInfo',{}).get('Deposit',None),
                    'CashInfoDueOnDelivery': deal.get('CashInfo',{}).get('DueOnDelivery',None),
                    'FinanceInfoMSRP': deal.get('FinanceInfo',{}).get('MSRP',None),
                    'FinanceInfoDeposit': deal.get('FinanceInfo',{}).get('Deposit',None),
                    'FinanceInfoCashOnDelivery': deal.get('FinanceInfo',{}).get('CashOnDelivery',None),
                    'FinanceInfoBank': deal.get('FinanceInfo',{}).get('Bank',None),
                    'FinanceInfoCode': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('Code',None),
                    'FinanceInfoName': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('Name',None),
                    'FinanceInfoAddress': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('Address',None),
                    'FinanceInfoPhone': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('Phone',None),
                    'FinanceInfoFax': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('Fax',None),
                    'FinanceInfoCity': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('City',None),
                    'FinanceInfoState': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('State',None),
                    'FinanceInfoZipCode': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('ZipCode',None),
                    'FinanceInfoBankNumber': deal.get('FinanceInfo',{}).get('BankInfo',{}).get('BankNumber',None),
                    'FinanceInfoRate': deal.get('FinanceInfo',{}).get('Rate',None),
                    'FinanceInfoEffectiveRate': deal.get('FinanceInfo',{}).get('EffectiveRate',None),
                    'FinanceInfoPaymentsPerYear': deal.get('FinanceInfo',{}).get('PaymentsPerYear',None),
                    'FinanceInfoPaymentTerm': deal.get('FinanceInfo',{}).get('PaymentTerm',None),
                    'FinanceInfoPaymentTermMonths': deal.get('FinanceInfo',{}).get('PaymentTermMonths',None),
                    'FinanceInfoAmortizationTerm': deal.get('FinanceInfo',{}).get('AmortizationTerm',None),
                    'FinanceInfoBalloon': deal.get('FinanceInfo',{}).get('Balloon',None),
                    'FinanceInfoBalanceToFinance': deal.get('FinanceInfo',{}).get('BalanceToFinance',None),
                    'FinanceInfoFinanceCharges': deal.get('FinanceInfo',{}).get('FinanceCharges',None),
                    'FinanceInfoTotalBalanceDue': deal.get('FinanceInfo',{}).get('TotalBalanceDue',None),
                    'FinanceInfoPaymentBase': deal.get('FinanceInfo',{}).get('PaymentBase',None),
                    'FinanceInfoPaymentBase': deal.get('FinanceInfo',{}).get('PaymentTaxes',[]),
                    'FinanceInfoPayment': deal.get('FinanceInfo',{}).get('Payment',None),
                    'FinanceInfoTerm': deal.get('FinanceInfo',{}).get('Term',None),
                    'FinanceInfoAPR': deal.get('FinanceInfo',{}).get('APR',None),
                    'LeaseInfoMSRP': deal.get('LeaseInfo',{}).get('MSRP',None),
                    'LeaseInfoCapTaxes': deal.get('LeaseInfo',{}).get('CapTaxes',[]),
                    'LeaseInfoCapSettings': deal.get('LeaseInfo',{}).get('CapSettings',[]),
                    'LeaseInfoCapCost': deal.get('LeaseInfo',{}).get('CapCost',None),
                    'LeaseInfoCashOnDelivery': deal.get('LeaseInfo',{}).get('CashOnDelivery',None),
                    'LeaseInfoCapReduction': deal.get('LeaseInfo',{}).get('CapReduction',None),
                    'LeaseInfoNetLease': deal.get('LeaseInfo',{}).get('NetLease',None),
                    'LeaseInfoResidualPercent': deal.get('LeaseInfo',{}).get('ResidualPercent',None),
                    'LeaseInfoResidualAmount': deal.get('LeaseInfo',{}).get('ResidualAmount',None),
                    'LeaseInfoInceptionMilesAllowed': deal.get('LeaseInfo',{}).get('InceptionMilesAllowed',None),
                    'LeaseInfoInceptionMileageRate': deal.get('LeaseInfo',{}).get('InceptionMileageRate',None),
                    'LeaseInfoInceptionMileageIncluded': deal.get('LeaseInfo',{}).get('InceptionMileageIncluded',None),
                    'LeaseInfoMileageCategory': deal.get('LeaseInfo',{}).get('MileageCategory',None),
                    'LeaseInfoMileageAllowed': deal.get('LeaseInfo',{}).get('MileageAllowed',None),
                    'LeaseInfoMileageExpected': deal.get('LeaseInfo',{}).get('MileageExpected',None),
                    'LeaseInfoMileageRate': deal.get('LeaseInfo',{}).get('MileageRate',None),
                    'LeaseInfoExcessMileageRate': deal.get('LeaseInfo',{}).get('ExcessMileageRate',None),
                    'LeaseInfoMileageCharges': deal.get('LeaseInfo',{}).get('MileageCharges',None),
                    'LeaseInfoResidualNet': deal.get('LeaseInfo',{}).get('ResidualNet',None),
                    'LeaseInfoResidualAdjustment': deal.get('LeaseInfo',{}).get('ResidualAdjustment',None),
                    'LeaseInfoDepreciation': deal.get('LeaseInfo',{}).get('Depreciation',None),
                    'LeaseInfoBank': deal.get('LeaseInfo',{}).get('Bank',None),
                    'BankInfoCode': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('Code',None),
                    'BankInfoName': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('Name',None),
                    'BankInfoAddress': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('Address',None),
                    'BankInfoPhone': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('Phone',None),
                    'BankInfoFax': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('Fax',None),
                    'BankInfoCity': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('City',None),
                    'BankInfoState': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('State',None),
                    'BankInfoZipCode': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('ZipCode',None),
                    'BankInfoBankNumber': deal.get('LeaseInfo',{}).get('BankInfo',{}).get('BankNumber',None),
                    'LeaseCalculationModel': deal.get('LeaseInfo',{}).get('LeaseCalculationModel',None),
                    'LeaseRate': deal.get('LeaseInfo',{}).get('Rate',None),
                    'LeaseEffectiveRate': deal.get('LeaseInfo',{}).get('EffectiveRate',None),
                    'LeasePaymentsPerYear': deal.get('LeaseInfo',{}).get('PaymentsPerYear',None),
                    'LeasePaymentTerm': deal.get('LeaseInfo',{}).get('PaymentTerm',None),
                    'LeasePaymentTermMonths': deal.get('LeaseInfo',{}).get('PaymentTermMonths',None),
                    'LeaseAmortizationTerm': deal.get('LeaseInfo',{}).get('AmortizationTerm',None),
                    'LeasePaymentBase': deal.get('LeaseInfo',{}).get('PaymentBase',None),
                    'LeasePaymentTaxes': deal.get('LeaseInfo',{}).get('PaymentTaxes', []),
                    'LeasePayment': deal.get('LeaseInfo',{}).get('Payment',None),
                    'LeaseInfoUpfrontTaxes': deal.get('LeaseInfo',{}).get('UpfrontTaxes',None),
                    'LeaseSecurityDeposit': deal.get('LeaseInfo',{}).get('SecurityDeposit',None),
                    'LeaseDriveOffLease': deal.get('LeaseInfo',{}).get('DriveOffLease',None),
                    'LeasePayableOnDelivery': deal.get('LeaseInfo',{}).get('PayableOnDelivery',None),
                    'LeaseTerm': deal.get('LeaseInfo',{}).get('Term',None),
                    'LeaseFinanceCharges': deal.get('LeaseInfo',{}).get('FinanceCharges',None),
                    'LeaseTotalPayments': deal.get('LeaseInfo',{}).get('TotalPayments',None),
                    'LeaseAPR': deal.get('LeaseInfo',{}).get('APR',None),
                    'GrossAmount': deal.get('Gross', {}).get('Reserve', {}).get('Amount', None),
                    'GrossBaseRate': deal.get('Gross', {}).get('Reserve', {}).get('BaseRate', None),
                    'GrossMidRate': deal.get('Gross', {}).get('Reserve', {}).get('MidRate', None),
                    'GrossFactor': deal.get('Gross', {}).get('Reserve', {}).get('Factor', None),
                    'VehicleGross': deal.get('Gross', {}).get('VehicleGross', None),
                    'AccessoryGross': deal.get('Gross', {}).get('AccessoryGross', None),
                    'FinanceGross': deal.get('Gross', {}).get('FinanceGross', None),
                    'DealGross': deal.get('Gross', {}).get('DealGross', None),
                    'GrossIncentives': deal.get('Gross', {}).get('Incentives', None),
                    'ProspectRef': deal.get('ProspectRef', None),
                    'DealTags': deal.get('DealTags', []),
                    'LeadRef': deal.get('LeadRef', None),
                    'LeadSource': deal.get('LeadSource', None),
                    'LeadType': deal.get('LeadType', None),
                    'ProspectType': deal.get('ProspectType', None),
                    'StatusInfo': deal.get('StatusInfo', []),
                    'SystemStatus': deal.get('SystemStatus', None),
                    'DealInterestType': deal.get('DealInterestType', None),
                    'DealCommissions': deal.get('DealCommissions', None),
                    'DealActivityInfo': deal.get('DealActivityInfo', []),
                    'CashOnDelivery': deal.get('CashOnDelivery', None),
                    'APR': deal.get('APR', None),
                    'RecordInsertTimestamp': pbsapi.record_insert_timestamp
                    })
        return deals_data

    #Function to parse Employees Data
    def parse_employee_response(self, response):
        employee_data = []
        # Append the response data to the employee_data list
        for employee in response.get('Employees', []):
            employee_data.append({
                'Id' : employee.get('Id', None),
                'EmployeeId' : employee.get('EmployeeId', None),
                'SerialNumber' : employee.get('SerialNumber', None),
                'UserName' : employee.get('UserName', None),
                'Password' : employee.get('Password', None),
                'PasswordHash' : employee.get('PasswordHash', None),
                'FirstName' : employee.get('FirstName', None),
                'LastName' : employee.get('LastName', None),
                'EmailAddress' : employee.get('EmailAddress', None),
                'Sales' : employee.get('Sales', None),
                'SalesmanNumber' : employee.get('SalesmanNumber', None),
                'SalesRole' : employee.get('SalesRole', None),
                'BDC' : employee.get('BDC', None),
                'FixedOps' : employee.get('FixedOps', None),
                'FixedOpsEmployeeNumber' : employee.get('FixedOpsEmployeeNumber', None),
                'FixedOpsRole' : employee.get('FixedOpsRole', None),
                'Technician' : employee.get('Technician', None),
                'TechnicianNumber' : employee.get('TechnicianNumber', None),
                'ManufacturerID' : employee.get('ManufacturerID', None),
                'DefaultShop' : employee.get('DefaultShop', None),
                'LastUpdate' : employee.get('LastUpdate', None),
                'Phone' : employee.get('Phone', None),
                'PhoneExtension' : employee.get('PhoneExtension', None),
                'Occupation' : employee.get('Occupation', None),
                'Signature' : employee.get('Signature', None),
                'DisplayName' : employee.get('DisplayName', None),
                'CellPhone' : employee.get('CellPhone', None),
                'CallTrakPin' : employee.get('CallTrakPin', None),
                'APSUserId' : employee.get('APSUserId', None),
                'IsInactive' : employee.get('IsInactive', None),
                'IsConnectEnabled' : employee.get('IsConnectEnabled', None),
                'IsDocSigningEnabled' : employee.get('IsDocSigningEnabled', None),
                'IsShuttleDriver' : employee.get('IsShuttleDriver', None),
                'IsMobileServiceArrival' : employee.get('IsMobileServiceArrival', None),
                'MobileServiceArrivalAccess' : employee.get('MobileServiceArrivalAccess', None),
                'IsVehicleLotValidationEnabled' : employee.get('IsVehicleLotValidationEnabled', None),
                'WebAppointmentsDisplayAsAdvisor' : employee.get('WebAppointmentsDisplayAsAdvisor', None),
                'DealershipPhoneNumbers' : employee.get('DealershipPhoneNumbers', []),
                'RecordInsertTimestamp': pbsapi.record_insert_timestamp
            })
        return employee_data

    #Function to parse Parts Invoices Data
    def parse_partsinvoice_response(self, response):
        partsinvoice_data = []
        # Append the response data to the partsinvoice_data list
        for inv in response.get('PartsInvoices', []):
            partsinvoice_data.append({
                'Id': inv.get('Id', None),
                'InvoiceId': inv.get('InvoiceId', None),
                'SerialNumber': inv.get('SerialNumber', None),
                'InvoiceNumber': inv.get('InvoiceNumber', None),
                'RawPartsInvoiceNumber': inv.get('RawPartsInvoiceNumber', None),
                'ContactRef': inv.get('ContactRef', None),
                'Reference': inv.get('Reference', None),
                'PurchaseOrderNumber': inv.get('PurchaseOrderNumber', None),
                'ChargeType': inv.get('ChargeType', None),
                'Status': inv.get('Status', None),
                'DateOpened': inv.get('DateOpened', None),
                'DateCashiered': inv.get('DateCashiered', None),
                'PartLines': inv.get('PartLines', []),
                'Summary_Discount': inv.get('Summary', {}).get('Discount', None),
                'Summary_Sales': inv.get('Summary', {}).get('Sales', None),
                'Summary_Tax1': inv.get('Summary', {}).get('Tax1', None),
                'Summary_Tax2': inv.get('Summary', {}).get('Tax2', None),
                'Summary_TotalInvoice': inv.get('Summary', {}).get('TotalInvoice', None),
                'Summary_TaxExempt': inv.get('Summary', {}).get('TaxExempt', None),
                'Summary_Freight': inv.get('Summary', {}).get('Freight', None),
                'Summary_RestockingFee': inv.get('Summary', {}).get('RestockingFee', None),
                'ShippingAddress_Name': inv.get('ShippingAddress', {}).get('Name', None),
                'ShippingAddress_City': inv.get('ShippingAddress', {}).get('City', None),
                'ShippingAddress_Province': inv.get('ShippingAddress', {}).get('Province', None),
                'QuoteReference': inv.get('QuoteReference', None),
                'LastUpdate': inv.get('LastUpdate', None),
                'Memo': inv.get('Memo', None),
                'RecordInsertTimestamp': pbsapi.record_insert_timestamp
            })
        return partsinvoice_data

    #Function to parse Parts Inventory Data
    def parse_partsinventory_response(self, response):
        partsinventory_data = []
        # Append the response data to the partsinventory_data list
        for part in response.get('Parts', []):
            partsinventory_data.append({
            'Id': part.get('Id', None),
            'PartId': part.get('PartId', None),
            'SerialNumber': part.get('SerialNumber', None),
            'PartNumber': part.get('PartNumber', None),
            'StrippedNumber': part.get('StrippedNumber', None),
            'SupersessionPart': part.get('SupersessionPart', None),
            'AlternateParts': part.get('AlternateParts', None),
            'Description': part.get('Description', None),
            'Comments': part.get('Comments', None),
            'Bin1': part.get('Bin1', None),
            'Bin2': part.get('Bin2', None),
            'PartGroup': part.get('Group', None),
            'Class': part.get('Class', None),
            'Source': part.get('Source', None),
            'Manufacturer': part.get('Manufacturer', None),
            'Supplier': part.get('Supplier', None),
            'PartMaster': part.get('PartMaster', None),
            'Status': part.get('Status', None),
            'PackageQuantity': part.get('PackageQuantity', None),
            'PricingOriginalPrice': part.get('Pricing', {}).get('OriginalPrice', None),
            'PricingListPrice': part.get('Pricing', {}).get('ListPrice', None),
            'PricingTradePrice': part.get('Pricing', {}).get('TradePrice', None),
            'PricingExchangePrice': part.get('Pricing', {}).get('ExchangePrice', None),
            'PricingCostPrice': part.get('Pricing', {}).get('CostPrice', None),
            'PricingJobberPrice': part.get('Pricing', {}).get('JobberPrice', None),
            'PricingFlatPrice': part.get('Pricing', {}).get('FlatPrice', None),
            'StockingBestStockingLevel': part.get('Stocking', {}).get('BestStockingLevel', None),
            'StockingMinimum': part.get('Stocking', {}).get('Minimum', None),
            'StockingMaximum': part.get('Stocking', {}).get('Maximum', None),
            'ManufacturerSuggestedStockingManufacturerManaged': part.get('ManufacturerSuggestedStocking', {}).get('ManufacturerManaged', None),
            'ManufacturerSuggestedStockingBestStockingLevel': part.get('ManufacturerSuggestedStocking', {}).get('BestStockingLevel', None),
            'ManufacturerSuggestedStockingReOrderPoint': part.get('ManufacturerSuggestedStocking', {}).get('ReOrderPoint', None),
            'ManufacturerSuggestedStockingMaximumStockLevel': part.get('ManufacturerSuggestedStocking', {}).get('MaximumStockLevel', None),
            'ManufacturerSuggestedStockingAdditionalInformation': part.get('ManufacturerSuggestedStocking', {}).get('AdditionalInformation', None),
            'ManufacturerSuggestedStockingProgramType': part.get('ManufacturerSuggestedStocking', {}).get('ProgramType', None),
            'ManufacturerSuggestedStockingManufacturerObsolete': part.get('ManufacturerSuggestedStocking', {}).get('ManufacturerObsolete', None),
            'ManufacturerSuggestedStockingManufacturerInfo': part.get('ManufacturerSuggestedStocking', {}).get('ManufacturerInfo', []),
            'OnHandTotal': part.get('OnHand', {}).get('Total', None),
            'OnHandAllocated': part.get('OnHand', {}).get('Allocated', None),
            'OnHandAvailable': part.get('OnHand', {}).get('Available', None),
            'OnHandOpenWork': part.get('OnHand', {}).get('OpenWork', None),
            'OnOrderTotal': part.get('OnOrder', {}).get('Total', None),
            'OnOrderAllocated': part.get('OnOrder', {}).get('Allocated', None),
            'OnOrderAvailable': part.get('OnOrder', {}).get('Available', None),
            'OnOrderPending': part.get('OnOrder', {}).get('Pending', None),
            'OnOrderBackOrder': part.get('OnOrder', {}).get('BackOrder', None),
            'SalesHits': part.get('SalesHits', []),
            'LastReceipt': part.get('LastReceipt', None),
            'LastSale': part.get('LastSale', None),
            'EntryDate': part.get('EntryDate', None),
            'LastUpdate': part.get('LastUpdate', None),
            'LastOrderDate': part.get('LastOrderDate', None),
            'LastPhysicalInventoryDate': part.get('LastPhysicalInventoryDate', None),
            'LastReceiptedQuantity': part.get('LastReceiptedQuantity', None),
            'OrderQuantityReceivedMTD': part.get('OrderQuantityReceivedMTD', None),
            'LastAdjusted': part.get('LastAdjusted', None),
            'LastLostSale': part.get('LastLostSale', None),
            'LastClosedSaleDate': part.get('LastClosedSaleDate', None),
            'PerJobQuantity': part.get('PerJobQuantity', None),
            'ReturnCode': part.get('ReturnCode', None),
            'CustomFields': part.get('CustomFields', []),
            'NextSupersessionPart': part.get('NextSupersessionPart', None),
            'RecordInsertTimestamp': pbsapi.record_insert_timestamp
            })
        return partsinventory_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List of serial numbers
serial_numbers = ['6277','7846']
pbsapi = PbsApi(username, password)

# Get today's date and set the time to 00:00:00.000000
today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

# Get yesterday's date by subtracting one day from today
yesterday_start = today - timedelta(days=1)

# Adjust yesterday's date to be till 23:59:59
yesterday_end = yesterday_start.replace(hour=23, minute=59, second=59, microsecond=0)

# Format the dates as strings in the desired format
since = yesterday_start.strftime('%Y-%m-%dT%H:%M:%S.0000000Z')
until = yesterday_end.strftime('%Y-%m-%dT%H:%M:%S.0000000Z')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Opcodes Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    # Define the JSON payload
    json_payload = {
    "SerialNumber":serial_number
    }
    opcode_api_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/OpCodeGet',
        data=json_payload)
    opcode_data = pbsapi.parse_opcode_response(opcode_api_response)

    # Define the schema
    opcode_schema = StructType([
            StructField("Id", StringType(), True),
            StructField("SerialNumber", StringType(), True),
            StructField("OpCodeId", StringType(), False),
            StructField("Code", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("FlatDollars", StringType(), True),
            StructField("FlatHours", StringType(), True),
            StructField("AllowedHours", StringType(), True),
            StructField("LabourEstimate", StringType(), True),
            StructField("PartsEstimate", StringType(), True),
            StructField("SkillCode", StringType(), True),
            StructField("RateCode", StringType(), True),
            StructField("LastUpdate", StringType(), True),
            StructField("IsWeb", BooleanType(), True),
            StructField("WebSequence", StringType(), True),
            StructField("GMCode", StringType(), True),
            StructField("IsGMOpCode", BooleanType(), True),
            StructField("IsDeleted", BooleanType(), True),
            StructField("DisplayInPulldown", BooleanType(), True),
            StructField("Category", StringType(), True),
            StructField("OpCodePriceCodes", StringType(), True),
            StructField("PartLines", StringType(), True),
            StructField("RecordInsertTimestamp", TimestampNTZType(), True)
        ])
    
    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/opcodes'

    unique_key = "OpCodeId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, opcode_data, opcode_schema, unique_key)

pbsapi.log.info("Get Opcode Job Completed Successfully") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Employees Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    # Define the JSON payload
    json_payload = {
        "SerialNumber": serial_number
    }
    employee_api_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/EmployeeGet',
        data=json_payload) 
    
    employee_data = pbsapi.parse_employee_response(employee_api_response)

    employee_schema = StructType([
        StructField('Id', StringType(), True),
        StructField('EmployeeId', StringType(), True),
        StructField('SerialNumber', StringType(), True),
        StructField('UserName', StringType(), True),
        StructField('Password', StringType(), True),
        StructField('PasswordHash', StringType(), True),
        StructField('FirstName', StringType(), True),
        StructField('LastName', StringType(), True),
        StructField('EmailAddress', StringType(), True),
        StructField('Sales', BooleanType(), True),
        StructField('SalesmanNumber', StringType(), True),
        StructField('SalesRole', StringType(), True),
        StructField('BDC', StringType(), True),
        StructField('FixedOps', BooleanType(), True),
        StructField('FixedOpsEmployeeNumber', StringType(), True),
        StructField('FixedOpsRole', StringType(), True),
        StructField('Technician', BooleanType(), True),
        StructField('TechnicianNumber', StringType(), True),
        StructField('ManufacturerID', StringType(), True),
        StructField('DefaultShop', StringType(), True),
        StructField('LastUpdate', StringType(), True),
        StructField('Phone', StringType(), True),
        StructField('PhoneExtension', StringType(), True),
        StructField('Occupation', StringType(), True),
        StructField('Signature', StringType(), True),
        StructField('DisplayName', StringType(), True),
        StructField('CellPhone', StringType(), True),
        StructField('CallTrakPin', StringType(), True),
        StructField('APSUserId', StringType(), True),
        StructField('IsInactive', BooleanType(), True),
        StructField('IsConnectEnabled', BooleanType(), True),
        StructField('IsDocSigningEnabled', BooleanType(), True),
        StructField('IsShuttleDriver', BooleanType(), True),
        StructField('IsMobileServiceArrival', BooleanType(), True),
        StructField('MobileServiceArrivalAccess', StringType(), True),
        StructField('IsVehicleLotValidationEnabled', BooleanType(), True),
        StructField('WebAppointmentsDisplayAsAdvisor', StringType(), True),
        StructField('DealershipPhoneNumbers', StringType(), True),
        StructField('RecordInsertTimestamp', TimestampNTZType(), True)
        ])

    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/employees'

    unique_key = "EmployeeId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, employee_data, employee_schema, unique_key)

pbsapi.log.info("Get Employees Job Completed Successfully") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Contacts Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    # Define the JSON payload
    json_payload = {
        "SerialNumber": serial_number,
        "ModifiedSince": since,
        "ModifiedUntil": until
    }
    contacts_api_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/ContactGet',
        data=json_payload)
    
    contacts_data = pbsapi.parse_contacts_response(contacts_api_response)

    contacts_schema = StructType([
        StructField('Id', StringType(), True),
        StructField('ContactId', StringType(), False),
        StructField('SerialNumber', StringType(), True),
        StructField('Code', StringType(), True),
        StructField('LastName', StringType(), True),
        StructField('FirstName', StringType(), True),
        StructField('Salutation', StringType(), True),
        StructField('MiddleName', StringType(), True),
        StructField('ContactName', StringType(), True),
        StructField('IsInactive', BooleanType(), True),
        StructField('IsBusiness', BooleanType(), True),
        StructField('ApartmentNumber', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('County', StringType(), True),
        StructField('State', StringType(), True),
        StructField('ZipCode', StringType(), True),
        StructField('BusinessPhone', StringType(), True),
        StructField('BusinessPhoneExt', StringType(), True),
        StructField('HomePhone', StringType(), True),
        StructField('CellPhone', StringType(), True),
        StructField('BusinessPhoneRawReverse', StringType(), True),
        StructField('HomePhoneRawReverse', StringType(), True),
        StructField('CellPhoneRawReverse', StringType(), True),
        StructField('FaxNumber', StringType(), True),
        StructField('EmailAddress', StringType(), True),
        StructField('Notes', StringType(), True),
        StructField('CriticalMemo', StringType(), True),
        StructField('BirthDate', StringType(), True),
        StructField('Gender', StringType(), True),
        StructField('DriverLicense', StringType(), True),
        StructField('DriversLicenseExpiry', StringType(), True),
        StructField('PreferredContactMethods', StringType(), True),
        StructField('LastUpdate', StringType(), True),
        StructField('CustomFields', StringType(), True),
        StructField('FleetType', StringType(), True),
        StructField('RelationshipType', StringType(), True),
        StructField('CommunicationPreferencesEmail', StringType(), True),
        StructField('CommunicationPreferencesPhone', StringType(), True),
        StructField('CommunicationPreferencesTextMessage', StringType(), True),
        StructField('CommunicationPreferencesLetter', StringType(), True),
        StructField('CommunicationPreferencesPreferred', StringType(), True),
        StructField('CommunicationPreferencesFollowUp', StringType(), True),
        StructField('CommunicationPreferencesMarketing', StringType(), True),
        StructField('CommunicationPreferencesThirdParty', StringType(), True),
        StructField('CommunicationPreferencesImplicitConsentDate', StringType(), True),
        StructField('SalesRepRef', StringType(), True),
        StructField('Language', StringType(), True),
        StructField('PayableAccount', StringType(), True),
        StructField('ReceivableAccount', StringType(), True),
        StructField('IsStatic', BooleanType(), True),
        StructField('PrimaryImageRef', StringType(), True),
        StructField('PayableAccounts', StringType(), True),
        StructField('ReceivableAccounts', StringType(), True),
        StructField('IsAPVendor', BooleanType(), True),
        StructField('IsARCustomer', BooleanType(), True),
        StructField('ManufacturerLoyaltyNumber', StringType(), True),
        StructField('MergedToContactRef', StringType(), True),
        StructField('DoNotLoadHistory', BooleanType(), True),
        StructField('RepairOrderRequiresPO', BooleanType(), True),
        StructField('RecordInsertTimestamp', TimestampNTZType(), True)
    ])

    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/contacts'

    unique_key = "ContactId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, contacts_data, contacts_schema, unique_key)

pbsapi.log.info("Get Contacts Job Completed Successfully") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Appointments Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    # Define the JSON payload
    json_payload = {
        "SerialNumber": serial_number,
        "AppointmentSince": since,
        "AppointmentUntil": until
    }
    appointments_api_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/AppointmentGet',
        data=json_payload) 
    
    appointments_data = pbsapi.parse_appointments_response(appointments_api_response)

    appointments_schema = StructType([
        StructField('Id', StringType(), True),
        StructField('AppointmentId', StringType(), False),
        StructField('SerialNumber', StringType(), True),
        StructField('AppointmentNumber', StringType(), True),
        StructField('RawAppointmentNumber', StringType(), True),
        StructField('Shop', StringType(), True),
        StructField('Advisor', StringType(), True),
        StructField('AdvisorRef', StringType(), True),
        StructField('BookingUser', StringType(), True),
        StructField('BookingUserRef', StringType(), True),
        StructField('Transportation', StringType(), True),
        StructField('ContactRef', StringType(), True),
        StructField('VehicleRef', StringType(), True),
        StructField('MileageIn', IntegerType(), True), 
        StructField('IsComeback', BooleanType(), True),
        StructField('IsWaiter', BooleanType(), True),
        StructField('AppointmentTime', StringType(), True),
        StructField('AppointmentTimeUTC', StringType(), True),
        StructField('PickupTime', StringType(), True),
        StructField('PickupTimeUTC', StringType(), True),
        StructField('RequestLines', StringType(), True), 
        StructField('DateOpened', StringType(), True),
        StructField('LastUpdate', StringType(), True),
        StructField('Status', StringType(), True),
        StructField('Notes', StringType(), True),
        StructField('Source', StringType(), True),
        StructField('PendingRequest', BooleanType(), True),
        StructField('CheckedIn', BooleanType(), True),
        StructField('Confirmed', BooleanType(), True),
        StructField('LeadRef', StringType(), True),
        StructField('NotifyType', StringType(), True),
        StructField('Tag', StringType(), True),
        StructField('FirstApptTimeAvailable', StringType(), True),
        StructField('RecordInsertTimestamp', TimestampNTZType(), True)
    ])

    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/appointments'

    unique_key = "AppointmentId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, appointments_data, appointments_schema, unique_key)

pbsapi.log.info("Get Appointments Job Completed Successfully") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Repair Orders Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    repairorder_json_payload = {
        "SerialNumber": serial_number
        # "CashieredSince": "2024-01-01T00:00:00.0000000Z",
        # "CashieredUntil": until
    }
    repairorders_api_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/RepairOrderGet',
        data=repairorder_json_payload)

    repairorders_data = pbsapi.parse_repairorders_response(repairorders_api_response)

    repairorders_schema = StructType([
        StructField('Id', StringType(), True),
        StructField('RepairOrderId', StringType(), True),
        StructField('SerialNumber', StringType(), True),
        StructField('RepairOrderNumber', StringType(), True),
        StructField('RawRepairOrderNumber', StringType(), True),
        StructField('DateOpened', StringType(), True),
        StructField('DateOpenedUTC', StringType(), True),
        StructField('DateCashiered', StringType(), True),
        StructField('DateCashieredUTC', StringType(), True),
        StructField('DatePromised', StringType(), True),
        StructField('DatePromisedUTC', StringType(), True),
        StructField('DateVehicleCompleted', StringType(), True),
        StructField('DateCustomerNotified', StringType(), True),
        StructField('CSR', StringType(), True),
        StructField('CSRRef', StringType(), True),
        StructField('BookingUser', StringType(), True),
        StructField('BookingUserRef', StringType(), True),
        StructField('ContactRef', StringType(), True),
        StructField('VehicleRef', StringType(), True),
        StructField('MileageIn', StringType(), True),
        StructField('MileageOut', StringType(), True),
        StructField('Tag', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('IsWaiter', BooleanType(), True),
        StructField('IsComeback', BooleanType(), True),
        StructField('Shop', StringType(), True),
        StructField('ChargeType', StringType(), True),
        StructField('PurchaseOrderNumber', StringType(), True),
        StructField('Transportation', StringType(), True),
        StructField('Status', StringType(), True),
        StructField('Memo', StringType(), True),
        StructField('MemoCustomerCopy', StringType(), True),
        StructField('AppointmentNumber', StringType(), True),
        StructField('AppointmentRef', StringType(), True),
        StructField('LastUpdate', StringType(), True),
        StructField('IsHardCopyPrinted', BooleanType(), True),
        StructField('Priority', StringType(), True),
        StructField('TodayPhoneNumber', StringType(), True),
        StructField('NotifyType', StringType(), True),
        StructField('IncludeInternalPricing', BooleanType(), True),
        StructField('VINInquiryPerformed', BooleanType(), True),
        StructField('SONote', StringType(), True),
        StructField('Requests', StringType(), True),
        StructField('CustomerSummaryLabour', StringType(), True),
        StructField('CustomerSummaryParts', StringType(), True),
        StructField('CustomerSummaryOilGas', StringType(), True),
        StructField('CustomerSummarySubletTow', StringType(), True),
        StructField('CustomerSummaryMisc', StringType(), True),
        StructField('CustomerSummaryEnvironment', StringType(), True),
        StructField('CustomerSummaryShopSupplies', StringType(), True),
        StructField('CustomerSummaryFreight', StringType(), True),
        StructField('CustomerSummaryWarrantyDeductible', StringType(), True),
        StructField('CustomerSummaryDiscount', StringType(), True),
        StructField('CustomerSummarySubTotal', StringType(), True),
        StructField('CustomerSummaryTax1', StringType(), True),
        StructField('CustomerSummaryTax2', StringType(), True),
        StructField('CustomerSummaryInvoiceTotal', StringType(), True),
        StructField('CustomerSummaryCustomerDeductible', StringType(), True),
        StructField('CustomerSummaryCustomerDeductibleBillableDescription', StringType(), True),
        StructField('CustomerSummaryGrandTotal', StringType(), True),
        StructField('CustomerSummaryStatus', StringType(), True),
        StructField('CustomerSummaryDateCashiered', StringType(), True),
        StructField('CustomerSummaryLabourDiscount', StringType(), True),
        StructField('CustomerSummaryPartDiscount', StringType(), True),
        StructField('CustomerSummaryServiceFeeTotal', StringType(), True),
        StructField('WarrantySummaryLabour', StringType(), True),
        StructField('WarrantySummaryParts', StringType(), True),
        StructField('WarrantySummaryOilGas', StringType(), True),
        StructField('WarrantySummarySubletTow', StringType(), True),
        StructField('WarrantySummaryMisc', StringType(), True),
        StructField('WarrantySummaryEnvironment', StringType(), True),
        StructField('WarrantySummaryShopSupplies', StringType(), True),
        StructField('WarrantySummaryFreight', StringType(), True),
        StructField('WarrantySummaryWarrantyDeductible', StringType(), True),
        StructField('WarrantySummaryDiscount', StringType(), True),
        StructField('WarrantySummarySubTotal', StringType(), True),
        StructField('WarrantySummaryTax1', StringType(), True),
        StructField('WarrantySummaryTax2', StringType(), True),
        StructField('WarrantySummaryInvoiceTotal', StringType(), True),
        StructField('WarrantySummaryCustomerDeductible', StringType(), True),
        StructField('WarrantySummaryCustomerDeductibleBillableDescription', StringType(), True),
        StructField('WarrantySummaryGrandTotal', StringType(), True),
        StructField('WarrantySummaryStatus', StringType(), True),
        StructField('WarrantySummaryDateCashiered', StringType(), True),
        StructField('WarrantySummaryLabourDiscount', StringType(), True),
        StructField('WarrantySummaryPartDiscount', StringType(), True),
        StructField('WarrantySummaryServiceFeeTotal', StringType(), True),
        StructField('InternalSummaryLabour', StringType(), True),
        StructField('InternalSummaryParts', StringType(), True),
        StructField('InternalSummaryOilGas', StringType(), True),
        StructField('InternalSummarySubletTow', StringType(), True),
        StructField('InternalSummaryMisc', StringType(), True),
        StructField('InternalSummaryEnvironment', StringType(), True),
        StructField('InternalSummaryShopSupplies', StringType(), True),
        StructField('InternalSummaryFreight', StringType(), True),
        StructField('InternalSummaryWarrantyDeductible', StringType(), True),
        StructField('InternalSummaryDiscount', StringType(), True),
        StructField('InternalSummarySubTotal', StringType(), True),
        StructField('InternalSummaryTax1', StringType(), True),
        StructField('InternalSummaryTax2', StringType(), True),
        StructField('InternalSummaryInvoiceTotal', StringType(), True),
        StructField('InternalSummaryCustomerDeductible', StringType(), True),
        StructField('InternalSummaryCustomerDeductibleBillableDescription', StringType(), True),
        StructField('InternalSummaryGrandTotal', StringType(), True),
        StructField('InternalSummaryStatus', StringType(), True),
        StructField('InternalSummaryDateCashiered', StringType(), True),
        StructField('InternalSummaryLabourDiscount', StringType(), True),
        StructField('InternalSummaryPartDiscount', StringType(), True),
        StructField('InternalSummaryServiceFeeTotal', StringType(), True),
        StructField('LoanerVehicleRef', StringType(), True),
        StructField('LoanerFriendlyId', StringType(), True),
        StructField('LoanerDatePickup', StringType(), True),
        StructField('LoanerDateDropOff', StringType(), True),
        StructField('LoanerOdomPickup', StringType(), True),
        StructField('LoanerOdomDropOff', StringType(), True),
        StructField('LoanerAgreementNumber', StringType(), True),
        StructField('LoanerComments', StringType(), True),
        StructField('PendingRequests', StringType(), True),
        StructField('DeferredRequests', StringType(), True),
        StructField('CancelledRequests', StringType(), True),
        StructField('RecordInsertTimestamp', TimestampNTZType(), True)
    ])

    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/repair_orders'

    unique_key = "RepairOrderId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, repairorders_data, repairorders_schema, unique_key)

pbsapi.log.info("Get Repair Orders Job Completed Successfully") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Vehicle Inventory Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    vehicle_json_payload = {
        "SerialNumber": serial_number,
        "ModifiedSince": since,
        "ModifiedUntil": until
        # "StatusList": "New,Used,Demo"
    }
    vehicle_api_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/VehicleGet',
        data=vehicle_json_payload)

    vehicles_data = pbsapi.parse_vehicle_response(vehicle_api_response)

    vehicles_schema = StructType([
        StructField('Id', StringType(), True),
        StructField('VehicleId', StringType(), True),
        StructField('SerialNumber', StringType(), True),
        StructField('StockNumber', StringType(), True),
        StructField('VIN', StringType(), True),
        StructField('LicenseNumber', StringType(), True),
        StructField('FleetNumber', StringType(), True),
        StructField('Status', StringType(), True),
        StructField('OwnerRef', StringType(), True),
        StructField('ModelNumber', StringType(), True),
        StructField('Make', StringType(), True),
        StructField('Model', StringType(), True),
        StructField('Trim', StringType(), True),
        StructField('VehicleType', StringType(), True),
        StructField('Year', StringType(), True),
        StructField('Odometer', StringType(), True),
        StructField('ExteriorColorCode', StringType(), True),
        StructField('ExteriorColorDescription', StringType(), True),
        StructField('InteriorColorCode', StringType(), True),
        StructField('InteriorColorDescription', StringType(), True),
        StructField('Engine', StringType(), True),
        StructField('Cylinders', StringType(), True),
        StructField('Transmission', StringType(), True),
        StructField('DriveWheel', StringType(), True),
        StructField('Fuel', StringType(), True),
        StructField('Weight', StringType(), True),
        StructField('InServiceDate', StringType(), True),
        StructField('LastServiceDate', StringType(), True),
        StructField('LastServiceMileage', StringType(), True),
        StructField('Lot', StringType(), True),
        StructField('LotDescription', StringType(), True),
        StructField('Category', StringType(), True),
        StructField('Options', StringType(), True),
        StructField('Refurbishments', StringType(), True),
        StructField('OrderInvoiceNumber', StringType(), True),
        StructField('OrderPrice', StringType(), True),
        StructField('OrderStatus', StringType(), True),
        StructField('OrderEta', StringType(), True),
        StructField('OrderDate', StringType(), True),
        StructField('OrderEstimatedCost', StringType(), True),
        StructField('OrderStatusDate', StringType(), True),
        StructField('OrderIgnitionKeyCode', StringType(), True),
        StructField('OrderDoorKeyCode', StringType(), True),
        StructField('OrderDescription', StringType(), True),
        StructField('OrderLocationStatus', StringType(), True),
        StructField('OrderLocationStatusDate', StringType(), True),
        StructField('MSR', StringType(), True),
        StructField('BaseMSR', StringType(), True),
        StructField('Retail', StringType(), True),
        StructField('DateReceived', StringType(), True),
        StructField('InternetPrice', StringType(), True),
        StructField('Lotpack', StringType(), True),
        StructField('Holdback', StringType(), True),
        StructField('InternetNotes', StringType(), True),
        StructField('Notes', StringType(), True),
        StructField('CriticalMemo', StringType(), True),
        StructField('IsCertified', BooleanType(), True),
        StructField('LastSaleDate', StringType(), True),
        StructField('LastUpdate', StringType(), True),
        StructField('AppraisedValue', StringType(), True),
        StructField('Warranties', StringType(), True),
        StructField('Freight', StringType(), True),
        StructField('Air', StringType(), True),
        StructField('Inventory', StringType(), True),
        StructField('IsInactive', BooleanType(), True),
        StructField('FloorPlanCode', StringType(), True),
        StructField('FloorPlanAmount', StringType(), True),
        StructField('InsuranceCompany', StringType(), True),
        StructField('InsurancePolicy', StringType(), True),
        StructField('InsuranceExpiryDate', StringType(), True),
        StructField('InsuranceAgentName', StringType(), True),
        StructField('InsuranceAgentPhoneNumber', StringType(), True),
        StructField('Body', StringType(), True),
        StructField('ShortVIN', StringType(), True),
        StructField('AdditionalDrivers', StringType(), True),
        StructField('OrderDetailsDistributor', StringType(), True),
        StructField('PrimaryImageRef', StringType(), True),
        StructField('HoldVehicleRef', StringType(), True),
        StructField('HoldFrom', StringType(), True),
        StructField('HoldUntil', StringType(), True),
        StructField('HoldUserRef', StringType(), True),
        StructField('HoldContactRef', StringType(), True),
        StructField('HoldComments', StringType(), True),
        StructField('SeatingCapacity', StringType(), True),
        StructField('DeliveryDate', StringType(), True),
        StructField('WarrantyExpiry', StringType(), True),
        StructField('IsConditionallySold', BooleanType(), True),
        StructField('SalesDivision', StringType(), True),
        StructField('StyleRef', StringType(), True),
        StructField('TotalCost', StringType(), True),
        StructField('BlueBookValue', StringType(), True),
        StructField('VideoURL', StringType(), True),
        StructField('ShowOnWeb', BooleanType(), True),
        StructField('PDI', FloatType(), True),
        StructField('Configuration', StringType(), True),
        StructField('DisplayTrim', StringType(), True),
        StructField('RecordInsertTimestamp', TimestampNTZType(), True)
    ])

    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/inventory'

    unique_key = "VehicleId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, vehicles_data, vehicles_schema, unique_key)

pbsapi.log.info("Get Vehicles Job Completed Successfully") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Deals Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    dealget_json_payload = {
        "SerialNumber":serial_number,
        "SoldSince": since,
        "SoldUntil": until
    }
    dealget_api_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/DealGet',
        data=dealget_json_payload)

    deals_data = pbsapi.parse_dealget_response(dealget_api_response)

    deals_schema = StructType([
        StructField('Id', StringType(), True),
        StructField('DealId', StringType(), True),
        StructField('SerialNumber', StringType(), True),
        StructField('DealKey', StringType(), True),
        StructField('DealType', StringType(), True),
        StructField('UserRoles', StringType(), True),
        StructField('CreationDate', StringType(), True),
        StructField('ContractDate', StringType(), True),
        StructField('PaymentDate', StringType(), True),
        StructField('DeliveryDate', StringType(), True),
        StructField('SystemDeliveryDate', StringType(), True),
        StructField('SoldDate', StringType(), True),
        StructField('DeliveryStepsCompleted', StringType(), True),
        StructField('Conditions', StringType(), True),
        StructField('Status', StringType(), True),
        StructField('SaleType', StringType(), True),
        StructField('TaxCode', StringType(), True),
        StructField('Notes', StringType(), True),
        StructField('AmortizationTerm', StringType(), True),
        StructField('PaymentTerm', StringType(), True),
        StructField('PaymentTermMonths', StringType(), True),
        StructField('PaymentsPerYear', StringType(), True),
        StructField('Price', StringType(), True),
        StructField('BuyerRef', StringType(), True),
        StructField('CoBuyerRefs', StringType(), True),
        StructField('LastUpdate', StringType(), True),
        StructField('Fees', StringType(), True),
        StructField('Accessories', StringType(), True),
        StructField('Warranties', StringType(), True),
        StructField('Protections', StringType(), True),
        StructField('Insurance', StringType(), True),
        StructField('Trades', StringType(), True),
        StructField('Rebates', StringType(), True),
        StructField('Allowances', StringType(), True),
        StructField('BackEndAllowances', StringType(), True),
        StructField('Adjustments', StringType(), True),
        StructField('Vehicles', StringType(), True),
        StructField('VehicleInsuranceAgent', StringType(), True),
        StructField('VehicleInsuranceAddress', StringType(), True),
        StructField('VehicleInsuranceCity', StringType(), True),
        StructField('VehicleInsuranceProvince', StringType(), True),
        StructField('VehicleInsurancePostalCode', StringType(), True),
        StructField('VehicleInsurancePhone', StringType(), True),
        StructField('VehicleInsuranceFax', StringType(), True),
        StructField('VehicleInsuranceCompany', StringType(), True),
        StructField('VehicleInsuranceSaleNumber', StringType(), True),
        StructField('VehicleInsurancePolicyNumber', StringType(), True),
        StructField('VehicleInsurancePolicyEffective', StringType(), True),
        StructField('VehicleInsurancePolicyExpiry', StringType(), True),
        StructField('VehicleInsuranceCollision', StringType(), True),
        StructField('VehicleInsuranceComprehensive', StringType(), True),
        StructField('VehicleInsuranceLiability', StringType(), True),
        StructField('CashInfoMSRP', StringType(), True),
        StructField('CashInfoTaxes', StringType(), True),
        StructField('CashInfoDeposit', StringType(), True),
        StructField('CashInfoDueOnDelivery', StringType(), True),
        StructField('FinanceInfoMSRP', StringType(), True),
        StructField('FinanceInfoDeposit', StringType(), True),
        StructField('FinanceInfoCashOnDelivery', StringType(), True),
        StructField('FinanceInfoBank', StringType(), True),
        StructField('FinanceInfoCode', StringType(), True),
        StructField('FinanceInfoName', StringType(), True),
        StructField('FinanceInfoAddress', StringType(), True),
        StructField('FinanceInfoPhone', StringType(), True),
        StructField('FinanceInfoFax', StringType(), True),
        StructField('FinanceInfoCity', StringType(), True),
        StructField('FinanceInfoState', StringType(), True),
        StructField('FinanceInfoZipCode', StringType(), True),
        StructField('FinanceInfoBankNumber', StringType(), True),
        StructField('FinanceInfoRate', StringType(), True),
        StructField('FinanceInfoEffectiveRate', StringType(), True),
        StructField('FinanceInfoPaymentsPerYear', StringType(), True),
        StructField('FinanceInfoPaymentTerm', StringType(), True),
        StructField('FinanceInfoPaymentTermMonths', StringType(), True),
        StructField('FinanceInfoAmortizationTerm', StringType(), True),
        StructField('FinanceInfoBalloon', StringType(), True),
        StructField('FinanceInfoBalanceToFinance', StringType(), True),
        StructField('FinanceInfoFinanceCharges', StringType(), True),
        StructField('FinanceInfoTotalBalanceDue', StringType(), True),
        StructField('FinanceInfoPaymentBase', StringType(), True),
        StructField('FinanceInfoPaymentTaxes', StringType(), True),
        StructField('FinanceInfoPayment', StringType(), True),
        StructField('FinanceInfoTerm', StringType(), True),
        StructField('FinanceInfoAPR', StringType(), True),
        StructField('LeaseInfoMSRP', StringType(), True),
        StructField('LeaseInfoCapTaxes', StringType(), True),
        StructField('LeaseInfoCapSettings', StringType(), True),
        StructField('LeaseInfoCapCost', StringType(), True),
        StructField('LeaseInfoCashOnDelivery', StringType(), True),
        StructField('LeaseInfoCapReduction', StringType(), True),
        StructField('LeaseInfoNetLease', StringType(), True),
        StructField('LeaseInfoResidualPercent', StringType(), True),
        StructField('LeaseInfoResidualAmount', StringType(), True),
        StructField('LeaseInfoInceptionMilesAllowed', StringType(), True),
        StructField('LeaseInfoInceptionMileageRate', StringType(), True),
        StructField('LeaseInfoInceptionMileageIncluded', StringType(), True),
        StructField('LeaseInfoMileageCategory', StringType(), True),
        StructField('LeaseInfoMileageAllowed', StringType(), True),
        StructField('LeaseInfoMileageExpected', StringType(), True),
        StructField('LeaseInfoMileageRate', StringType(), True),
        StructField('LeaseInfoExcessMileageRate', StringType(), True),
        StructField('LeaseInfoMileageCharges', StringType(), True),
        StructField('LeaseInfoResidualNet', StringType(), True),
        StructField('LeaseInfoResidualAdjustment', StringType(), True),
        StructField('LeaseInfoDepreciation', StringType(), True),
        StructField('LeaseInfoBank', StringType(), True),
        StructField('BankInfoCode', StringType(), True),
        StructField('BankInfoName', StringType(), True),
        StructField('BankInfoAddress', StringType(), True),
        StructField('BankInfoPhone', StringType(), True),
        StructField('BankInfoFax', StringType(), True),
        StructField('BankInfoCity', StringType(), True),
        StructField('BankInfoState', StringType(), True),
        StructField('BankInfoZipCode', StringType(), True),
        StructField('BankInfoBankNumber', StringType(), True),
        StructField('LeaseCalculationModel', StringType(), True),
        StructField('LeaseRate', StringType(), True),
        StructField('LeaseEffectiveRate', StringType(), True),
        StructField('LeasePaymentsPerYear', StringType(), True),
        StructField('LeasePaymentTerm', StringType(), True),
        StructField('LeasePaymentTermMonths', StringType(), True),
        StructField('LeaseAmortizationTerm', StringType(), True),
        StructField('LeasePaymentBase', StringType(), True),
        StructField('LeasePaymentTaxes', StringType(), True),
        StructField('LeasePayment', StringType(), True),
        StructField('LeaseInfoUpfrontTaxes', StringType(), True),
        StructField('LeaseSecurityDeposit', StringType(), True),
        StructField('LeaseDriveOffLease', StringType(), True),
        StructField('LeasePayableOnDelivery', StringType(), True),
        StructField('LeaseTerm', StringType(), True),
        StructField('LeaseFinanceCharges', StringType(), True),
        StructField('LeaseTotalPayments', StringType(), True),
        StructField('LeaseAPR', StringType(), True),
        StructField('GrossAmount', StringType(), True),
        StructField('GrossBaseRate', StringType(), True),
        StructField('GrossMidRate', StringType(), True),
        StructField('GrossFactor', StringType(), True),
        StructField('VehicleGross', StringType(), True),
        StructField('AccessoryGross', StringType(), True),
        StructField('FinanceGross', StringType(), True),
        StructField('DealGross', StringType(), True),
        StructField('GrossIncentives', StringType(), True),
        StructField('ProspectRef', StringType(), True),
        StructField('DealTags', StringType(), True),
        StructField('LeadRef', StringType(), True),
        StructField('LeadSource', StringType(), True),
        StructField('LeadType', StringType(), True),
        StructField('ProspectType', StringType(), True),
        StructField('StatusInfo', StringType(), True),
        StructField('SystemStatus', StringType(), True),
        StructField('DealInterestType', StringType(), True),
        StructField('DealCommissions', StringType(), True),
        StructField('DealActivityInfo', StringType(), True),
        StructField('CashOnDelivery', FloatType(), True), 
        StructField('APR', FloatType(), True), 
        StructField('RecordInsertTimestamp', TimestampNTZType(), True)
    ])

    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/deals'

    unique_key = "DealId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, deals_data, deals_schema, unique_key)

pbsapi.log.info("Deal Get Job Completed Successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Parts Invoices Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    partsinvoice_json_payload = {
     "SerialNumber": serial_number,
     "CashieredSince": since,
     "CashieredUntil": until
    }
    partsinvoice_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/PartsInvoiceGet',
        data=partsinvoice_json_payload)

    partsinvoice_data = pbsapi.parse_partsinvoice_response(partsinvoice_response)

    partsinvoice_schema = StructType([
        StructField('Id', StringType(), True),
        StructField('InvoiceId', StringType(), True),
        StructField('SerialNumber', StringType(), True),
        StructField('InvoiceNumber', StringType(), True),
        StructField('RawPartsInvoiceNumber', StringType(), True),
        StructField('ContactRef', StringType(), True),
        StructField('Reference', StringType(), True),
        StructField('PurchaseOrderNumber', StringType(), True),
        StructField('ChargeType', StringType(), True),
        StructField('Status', StringType(), True),
        StructField('DateOpened', StringType(), True),
        StructField('DateCashiered', StringType(), True),
        StructField('PartLines', StringType(), True),
        StructField('Summary_Discount', StringType(), True),
        StructField('Summary_Sales', StringType(), True),
        StructField('Summary_Tax1', StringType(), True),
        StructField('Summary_Tax2', StringType(), True),
        StructField('Summary_TotalInvoice', StringType(), True),
        StructField('Summary_TaxExempt', StringType(), True),
        StructField('Summary_Freight', StringType(), True),
        StructField('Summary_RestockingFee', StringType(), True),
        StructField('ShippingAddress_Name', StringType(), True),
        StructField('ShippingAddress_City', StringType(), True),
        StructField('ShippingAddress_Province', StringType(), True),
        StructField('QuoteReference', StringType(), True),
        StructField('LastUpdate', StringType(), True),
        StructField('Memo', StringType(), True),
        StructField('RecordInsertTimestamp', TimestampNTZType(), True)
    ])

    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/parts_invoices'

    unique_key = "InvoiceId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, partsinvoice_data, partsinvoice_schema, unique_key)

pbsapi.log.info("Parts Invoice Get Job Completed Successfully") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Parts Inventory Job**

# CELL ********************

# Iterate over the list of serial numbers
for serial_number in serial_numbers:
    parts_inventory_json_payload = {
     "SerialNumber": serial_number,
     "SaleSince": since,
     "SaleUntil": until
    }
    parts_inventory_response = pbsapi.make_post_api_call(url='https://partnerhub.pbsdealers.com/json/reply/PartsInventoryGet',
        data=parts_inventory_json_payload)
    partsinventory_data = pbsapi.parse_partsinventory_response(parts_inventory_response)

    partsinventory_schema = StructType([
        StructField('Id', StringType(), True),
        StructField('PartId', StringType(), True),
        StructField('SerialNumber', StringType(), True),
        StructField('PartNumber', StringType(), True),
        StructField('StrippedNumber', StringType(), True),
        StructField('SupersessionPart', StringType(), True),
        StructField('AlternateParts', StringType(), True),
        StructField('Description', StringType(), True),
        StructField('Comments', StringType(), True),
        StructField('Bin1', StringType(), True),
        StructField('Bin2', StringType(), True),
        StructField('PartGroup', StringType(), True),
        StructField('Class', StringType(), True),
        StructField('Source', StringType(), True),
        StructField('Manufacturer', StringType(), True),
        StructField('Supplier', StringType(), True),
        StructField('PartMaster', StringType(), True),
        StructField('Status', StringType(), True),
        StructField('PackageQuantity', StringType(), True),
        StructField('PricingOriginalPrice', StringType(), True),
        StructField('PricingListPrice', StringType(), True),
        StructField('PricingTradePrice', StringType(), True),
        StructField('PricingExchangePrice', StringType(), True),
        StructField('PricingCostPrice', StringType(), True),
        StructField('PricingJobberPrice', StringType(), True),
        StructField('PricingFlatPrice', StringType(), True),
        StructField('StockingBestStockingLevel', StringType(), True),
        StructField('StockingMinimum', StringType(), True),
        StructField('StockingMaximum', StringType(), True),
        StructField('ManufacturerSuggestedStockingManufacturerManaged', StringType(), True),
        StructField('ManufacturerSuggestedStockingBestStockingLevel', StringType(), True),
        StructField('ManufacturerSuggestedStockingReOrderPoint', StringType(), True),
        StructField('ManufacturerSuggestedStockingMaximumStockLevel', StringType(), True),
        StructField('ManufacturerSuggestedStockingAdditionalInformation', StringType(), True),
        StructField('ManufacturerSuggestedStockingProgramType', StringType(), True),
        StructField('ManufacturerSuggestedStockingManufacturerObsolete', StringType(), True),
        StructField('ManufacturerSuggestedStockingManufacturerInfo', StringType(), True),
        StructField('OnHandTotal', StringType(), True),
        StructField('OnHandAllocated', StringType(), True),
        StructField('OnHandAvailable', StringType(), True),
        StructField('OnHandOpenWork', StringType(), True),
        StructField('OnOrderTotal', StringType(), True),
        StructField('OnOrderAllocated', StringType(), True),
        StructField('OnOrderAvailable', StringType(), True),
        StructField('OnOrderPending', StringType(), True),
        StructField('OnOrderBackOrder', StringType(), True),
        StructField('SalesHits', StringType(), True),
        StructField('LastReceipt', StringType(), True),
        StructField('LastSale', StringType(), True),
        StructField('EntryDate', StringType(), True),
        StructField('LastUpdate', StringType(), True),
        StructField('LastOrderDate', StringType(), True),
        StructField('LastPhysicalInventoryDate', StringType(), True),
        StructField('LastReceiptedQuantity', StringType(), True),
        StructField('OrderQuantityReceivedMTD', StringType(), True),
        StructField('LastAdjusted', StringType(), True),
        StructField('LastLostSale', StringType(), True),
        StructField('LastClosedSaleDate', StringType(), True),
        StructField('PerJobQuantity', StringType(), True),
        StructField('ReturnCode', StringType(), True),
        StructField('CustomFields', StringType(), True),
        StructField('NextSupersessionPart', StringType(), True),
        StructField('RecordInsertTimestamp', TimestampNTZType(), True)
    ])


    delta_table_path = 'abfss://cf6eae1e-540c-45e8-b1bd-ed425d395cfd@onelake.dfs.fabric.microsoft.com/76325447-8576-4ab5-a58b-6dd9319e8be7/Tables/pbs/parts_inventory'

    unique_key = "PartId"

    # Call Upsert Function to upsert data into Lakehouse
    pbsapi.upsert_dicts_to_lakehouse(spark, delta_table_path, partsinventory_data, partsinventory_schema, unique_key)

pbsapi.log.info("Get Parts Inventory Job Completed Successfully") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
