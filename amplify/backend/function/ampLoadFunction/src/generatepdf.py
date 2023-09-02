
# INVOICE_FIRST_PAGE.
def invoiceTemplate(invoice_data):
    print("InvoiceTESTING", invoice_data)
    invoice_first_page = f"""
        <!DOCTYPE html>
<html lang="en">

<head>
    <title>Invoice Document</title>
</head>

<body style="margin:auto; font-family: 'Roboto', sans-serif; color: #000; font-size: 16px;">
    <table style="width:700; margin: auto; padding: 20px;     border: none" cellpadding="0" cellspacing="0">


        <tr>
            <td colspan="3" style="padding: 0;  border: none">
                <table style="width:100" cellpadding="0" cellspacing="0">
                    <tr>
                        <td style="padding: 0;  border: none; vertical-align: top; width: 75;">
                            <P style="font-weight: 600; margin: 0; line-height: 22px;">
                                MODEL TRANSPORT LLC <br>9304 FOREST LN SUITE 236 <br> DALLAS, TX 75243</P>
                        </td>
                        <td style="padding: 0;  border: none; vertical-align: top;">
                            <h2 style="margin: 0; color: #393939;">INVIOCE</h2>
                            <p style="margin: 7px 0; ">Date: Feb 22, 2023</p>
                            <h4 style="margin: 0;     font-size: 22px;">Inv# 4458</h4>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>

        <tr>
            <td colspan="3" style="padding: 0;  border: none">
                <p style="font-weight: 500; line-height: 22px;">
                    TEL# 323-674-6094 <br> FAX# <br> Email: MODEL TRANSPORT18@GMAIL.COM
                </p>
            </td>
        </tr>

        <tr>
            <td colspan=" 3 " style="padding: 0; border: none; color: #393939; line-height: 28px;">
                <h2 style="margin: 0;">BILL TO:</h2>
                <h2 style="margin: 0;">SPOT</h2>
                <p style="margin: 0; font-weight: 500;">141 SOUTH MERIDIAN STREET INDIANPOLIS, IN 46225</p>
            </td>
        </tr>





        <tr>
            <td colspan="3 " style="padding: 0; border: none; padding-top: 40px; ">
                <table style="width:100" cellpadding="0 " cellspacing="0 " border="1 ">
                    <thead>
                        <tr>
                            <th style="padding: 5px; background-color: #dddddd; ">SHIPMENT DATE </th>
                            <th style="padding: 5px; background-color: #dddddd;">LOAD NO.</th>
                            <th style="padding: 5px; background-color: #dddddd;">TRUCK#</th>
                            <th style="padding: 5px; background-color: #dddddd;">TRAILER#</th>
                            <th style="padding: 5px; background-color: #dddddd;">P O NO.</th>
                            <th style="padding: 5px; background-color: #dddddd;">TERMS</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td style="text-align: center; padding: 5px; ">02/13/2023</td>
                            <td style="text-align: center; padding: 5px; ">S1560741</td>
                            <td style="text-align: center; padding: 5px; ">900</td>
                            <td style="text-align: center; padding: 5px; ">HOOK-</td>
                            <td style="text-align: center; padding: 5px; "></td>
                            <td style="text-align: center; padding: 5px; ">Net 15 Days</td>
                        </tr>
                    </tbody>
                </table>
            </td>
        </tr>

        <tr>
            <td colspan="3 " style="padding: 0; border: none; padding-top: 40px; ">
                <table style="width:100" cellpadding="0 " cellspacing="0 ">
                    <thead>
                        <tr>
                            <th style="padding: 5px; background-color: #dddddd;">QUANTITY </th>
                            <th style="padding: 5px; background-color: #dddddd; text-align: left;">DESCRIPTION</th>
                            <th style="padding: 5px; background-color: #dddddd;">UNIT PRICE</th>
                            <th style="padding: 5px; background-color: #dddddd;">TOTAL</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td style="text-align: center; padding: 5px;"></td>
                            <td style="text-align: left; padding: 5px;">LEWISVILLE, TX TO LANCASTER, PA </td>
                            <td style="text-align: center; padding: 5px;"></td>
                            <td style="text-align: center; padding: 5px;">$2,600.00</td>
                        </tr>
                    </tbody>
                </table>
            </td>
        </tr>


        <tr>
            <td colspan=" 3 " style="padding: 0; border: none;">
                <div style="margin: 100px 40px 40px 40px; background-color: #dddddd;">
                    <h2 style="margin: 0; text-align: center; color: #d8230b;font-size: 16px; line-height: 25px; padding: 5px 0;">
                        NOTICE OF ASSIGNMENT <br> THIS INVOICE HAS BEEN ASSIGNED TO, <br> AND MUST BE PAID DIRECTLY TO: <br> PHOENIX CAPITAL GROUP, <br> LLC P.O.BOX 1415 <br> DES MOINES, IA 50305 <br> Tel: 623-298-3466
                    </h2>
                </div>

            </td>
        </tr>


        <tr>
            <td colspan="3" style="padding: 0;  border: none">
                <table style="width:100" cellpadding="0" cellspacing="0">
                    <tr>
                        <td style="padding: 0;  border: none; vertical-align: top; width: 60;">

                        </td>
                        <td style="padding: 0;  border: none; vertical-align: top;">
                            <table cellpadding="0 " cellspacing="0 ">
                                <tbody>
                                    <tr>
                                        <td style="padding: 5px; background-color: #dddddd; text-align: right; font-weight: 600;">LINE HAUL</td>
                                        <td style="text-align: right; padding: 5px;  padding-left: 20px;"></td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 5px; background-color: #dddddd; text-align: right; font-weight: 600;">FSC</td>
                                        <td style="text-align: right; padding: 5px;  padding-left: 20px;"></td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 5px; background-color: #dddddd; text-align: right; font-weight: 600;">STOP OFF</td>
                                        <td style="text-align: right; padding: 5px;  padding-left: 20px;"></td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 5px; background-color: #dddddd; text-align: right; font-weight: 600;">UNLOADING</td>
                                        <td style="text-align: right; padding: 5px;  padding-left: 20px;"></td>
                                    </tr>
                                    <tr>
                                        <td style="padding: 5px; background-color: #dddddd; text-align: right; font-weight: 600;">TOTAL DUE</td>
                                        <td style="text-align: right; padding: 5px; padding-left: 20px;  font-weight: 600;"> $2,600.00 </td>
                                    </tr>
                                </tbody>
                            </table>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>

    </table>
</body>

</html>
        """
    return invoice_first_page



# 
# <html>
# <head>
# <meta name="viewport" content="width=device-width, initial-scale=1">
# <style>
# * {
#   box-sizing: border-box;
# }

# /* Create two equal columns that floats next to each other */
# .column {
#   float: left;
#   width: 50%;
#   padding: 10px;
#   height: 150px; /* Should be removed. Only for demonstration */
# }

# /* Clear floats after the columns */
# .row:after {
#   content: "";
#   display: table;
#   clear: both;
# }
# </style>
# </head>
# <body>

# <div class="row">
#   <div class="column" style="background-color:#aaa;">
#     <h4>MODEL TRANSPORT LLC</h2>
#     <h4>MODEL TRANSPORT LLC</h2>
#     <h4>MODEL TRANSPORT LLC</h2>
#   </div>
#   <div class="column" style="background-color:#aaa;">
#     <h4>MODEL TRANSPORT LLC</h2>
#     <h4>MODEL TRANSPORT LLC</h2>
#     <h4>MODEL TRANSPORT LLC</h2>
#   </div>
# </div>
# <div class="row">
#   <div class="column" style="background-color:#aaa;">
#     <h4>MODEL TRANSPORT LLC</h2>
#     <h4>MODEL TRANSPORT LLC</h2>
#     <h4>MODEL TRANSPORT LLC</h2>
#   </div>

# </div>
# <div class="row">
#   <div class="column" style="background-color:#aaa;">
#     <h4>MODEL TRANSPORT LLC</h2>
#     <h4>MODEL TRANSPORT LLC</h2>
#     <h4>MODEL TRANSPORT LLC</h2>
#   </div>

# </div>

# </body>
# </html>
