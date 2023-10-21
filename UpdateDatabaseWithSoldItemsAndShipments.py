import csv
import sqlite3
from datetime import datetime
from decimal import Decimal

databaseFile = 'Inventory Finances Database.sqlite'
errorLogFile = 'EbayReports\Ebay SQLite Error Log.log'
shipmentOutID_orderNumberMap = {}

def createLog(error_message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(errorLogFile, 'a') as errorFile:
        errorFile.write(f"[{timestamp}] {error_message}\n")

def findCarrierInfo(shippingService):
    if 'USPS' in shippingService.upper():
        carrier = 'USPS'
    elif 'FEDEX' in shippingService.upper():
        carrier = 'FedEx'
    elif 'UPS' in shippingService.upper():
        carrier = 'UPS'
    elif 'DHL' in shippingService.upper():
        carrier = 'DHL'
    elif 'AMAZON' in shippingService.upper():
        carrier = 'Amazon'
    else:
        carrier = 'Not Set'

    return carrier

def parse_date(date_str):
    try:
        # Try parsing 'Sep-30-23' format
        return datetime.strptime(date_str, '%b-%d-%y').strftime('%Y%m%d')
    except ValueError:
        try:
            # Try parsing 'Jul 3, 2023' format
            return datetime.strptime(date_str, '%b %d, %Y').strftime('%Y%m%d')
        except ValueError:
            createLog('Cannot parse the date correctly')
            return None


def findItemType(customLabel):
    if customLabel.startswith('G'):
        itemType = 'GameInventoryItem'
    elif customLabel.startswith('C'):
        itemType = 'ConsoleInventoryItem'
    elif customLabel.startswith('A'):
        itemType = 'AccessoryInventoryItem'
    elif customLabel.startswith('M'):
        itemType = 'MiscInventoryItem'
    else:
        itemType = 'Null'
    return itemType

def createShipmentOutEntries(connection):
    # Connect to the SQLite database
    with connection:
        cursor = connection.cursor()

        ebayReport = 'EbayReports\Ebay Orders Report.csv'


        # Open the CSV file and read its contents
        with open(ebayReport, 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)

            for row in csvreader:
                # Extract information from the CSV columns
                shippedDate = row['Shipped On Date']
                shippedDate = parse_date(shippedDate)
                trackingNumber = row['Tracking Number']
                if trackingNumber == '':
                    trackingNumber = None
                shippingService = row['Shipping Service']
                orderNumber = row['Order Number']

                carrier = findCarrierInfo(shippingService)

                try:
                    # Create an entry in the ShipmentsOut table
                    cursor.execute(
                        f"INSERT INTO ShipmentsOut (ShippedDate, TrackingNumber, Carrier) "
                        f"VALUES (?, ?, ?)",
                        (shippedDate, trackingNumber, carrier)
                    )


                    # Get the shipmentOutID of the entry
                    cursor.execute("SELECT last_insert_rowid()")
                    shipmentOutID = cursor.fetchone()[0]

                    shipmentOutID_orderNumberMap[orderNumber] = shipmentOutID


                except sqlite3.Error as e:
                    createLog(f"SQLite Error on shipmentTable: {str(e)} - Order Number: {orderNumber} - Tracking Number: {trackingNumber} - Carrier: {carrier}\n")

        connection.commit()
        print("Entries added to ShipmentsOut table successfully.")

def createSalesEntries(connection):
    # Connect to the SQLite database
    with connection:    
        cursor = connection.cursor()

        ebayReport = 'EbayReports\Ebay Transaction Report.csv'

        # Open the CSV file and read its contents
        with open(ebayReport, 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)

            for row in csvreader:
                type = row['Type']

                if type == 'Order':

                    # Extract information from the CSV columns
                    date = row['Transaction creation date']
                    date = parse_date(date)
                    marketplace = 'Ebay'
                    platformSaleID = row['Order number']
                    shipmentOutID = shipmentOutID_orderNumberMap.get(platformSaleID)
                    orderSubtotal = int(Decimal(row['Item subtotal']) * 100)
                    shippingPaidToMe = int(Decimal(row['Shipping and handling']) * 100)
                    
                    platformFeeFixed = int(Decimal(row['Final Value Fee - fixed']) * 100)
                    platformFeeVariable = int(Decimal(row['Final Value Fee - variable']) * 100)
                    platformFee = platformFeeFixed + platformFeeVariable

                    customLabel = row['Custom label']

                    try:
                        # Create an entry in the Sales table
                        cursor.execute(
                            "INSERT INTO Sales (Date, Marketplace, PlatformSaleID, ShipmentOutID, OrderSubtotal, ShippingPaidToMe, PlatformFee) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (date, marketplace, platformSaleID, shipmentOutID, orderSubtotal, shippingPaidToMe, platformFee)
                        )

                        # Get the SaleID of the entry
                        cursor.execute("SELECT last_insert_rowid()")
                        saleID = cursor.fetchone()[0]

                        createSaleItemsEntries(connection, saleID, date, customLabel, orderSubtotal)

                        connection.commit()


                    except sqlite3.Error as e:
                        createLog(f"SQLite Error on sales table: {str(e)} - Order Number: {platformSaleID}\n")

                elif 'Shipping label'.strip().lower() in type.strip().lower():

                    marketplace = 'Ebay'
                    platformSaleID = row['Order number']
                    shippingCost = int(Decimal(row['Net amount']) * 100)

                    #find the entry in the table where the marketplace and the platformSaleID match. Need to modify the values in it
                    cursor.execute(
                        "SELECT * FROM Sales WHERE Marketplace = ? AND PlatformSaleID = ?",
                        (marketplace, platformSaleID)
                    )
                    existing_row = cursor.fetchone()

                    if existing_row:
                        try:
                            cursor.execute(
                                "UPDATE Sales SET ShippingCost = ? "
                                "WHERE Marketplace = ? AND PlatformSaleID = ?",
                                (shippingCost, marketplace, platformSaleID)
                            )
                            connection.commit()
                        except sqlite3.Error as e:
                            createLog(f"SQLite Error on sales table: {str(e)} - Order Number: {platformSaleID}\n")
                else:
                    continue

        print("Entries added to ShipmentsOut table successfully.")

def createSaleItemsEntries(connection, saleID, date, customLabel, orderSubtotal):
    # Connect to the SQLite database
    with connection:
        cursor = connection.cursor()

        itemType = findItemType(customLabel)
        itemID = customLabel[1:]

        try:
            if itemType == 'GameInventoryItem':
                cursor.execute(
                    "INSERT INTO SaleItems (SaleID, Date, GameItemID, ConsoleItemID, AccessoryItemID, MiscItemID, SoldPrice) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (saleID, date, itemID, None, None, None, orderSubtotal)
                )
            elif itemType == 'ConsoleInventoryItem':
                        cursor.execute(
                    "INSERT INTO SaleItems (SaleID, Date, GameItemID, ConsoleItemID, AccessoryItemID, MiscItemID, SoldPrice) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (saleID, date, None, itemID, None, None, orderSubtotal)
                )
            elif itemType == 'AccessoryInventoryItem':
                        cursor.execute(
                    "INSERT INTO SaleItems (SaleID, Date, GameItemID, ConsoleItemID, AccessoryItemID, MiscItemID, SoldPrice) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (saleID, date, None, None, itemID, None, orderSubtotal)
                )
            elif itemType == 'MiscInventoryItem':
                    cursor.execute(
                "INSERT INTO SaleItems (SaleID, Date, GameItemID, ConsoleItemID, AccessoryItemID, MiscItemID, SoldPrice) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (saleID, date, None, None, None, itemID, orderSubtotal)
            )
            else:
                return
            
        except sqlite3.Error as e:
            createLog(f"SQLite Error on saleItems table: {str(e)} - Sale ID: {saleID} - ID: {customLabel}\n")

        print('Added saleitem')

def main():
    connection = sqlite3.connect(databaseFile)
    createShipmentOutEntries(connection)
    createSalesEntries(connection)

if __name__ == '__main__':
    main()