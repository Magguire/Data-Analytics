# import pymysql for making connection
import pymysql
import pandas as pd
from app import app
from config import mysql
import pickle
import scipy
from datetime import datetime
import sklearn

# import jsonify to return json response
from flask import flash, json, jsonify, request

# load model
with open('paybill_classifier', 'rb') as training_model:
    model = pickle.load(training_model)

# load paybill vectorizer
with open('paybill_vectorizer', 'rb') as training_vectorizer:
    paybill_vect = pickle.load(training_vectorizer)

# load description vectorizer
with open('desc_vectorizer', 'rb') as training_vectorizer:
    desc_vect = pickle.load(training_vectorizer)


# function to convert datetime
def toDate(dateString):
    date = datetime.strptime(dateString, "%Y-%m-%d").date()
    return date.isoformat()


# get fullname and filename from mpesa_transactions summary
@app.route('/get_filename', methods=['GET'])
def file_name():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT id, fileName FROM mpesa_transactions_summary")
        filename = cur.fetchall()
        response = jsonify(filename)
        response.status_code = 200
        return response

    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

    # get date from mpesa_transactions summary


@app.route('/get_dates', methods=['GET'])
def dates():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')

        if startDate and endDate:
            cur.execute(
                f"SELECT DISTINCT fileName,DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions WHERE DATE(transactionDate) BETWEEN {startDate} AND {endDate}")
            date = pd.DataFrame(cur.fetchall())
            date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
            date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
            date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
            date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
        elif endDate:
            cur.execute(
                f"SELECT DISTINCT DATE(transactionDate) AS endDate FROM mpesa_transactions WHERE DATE(transactionDate) <= {endDate}")
            date = pd.DataFrame(cur.fetchall())
            date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
            date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
        elif startDate:
            cur.execute(
                f"SELECT DISTINCT DATE(transactionDate) AS startDate FROM mpesa_transactions WHERE DATE(transactionDate) >= {startDate}")
            date = pd.DataFrame(cur.fetchall())
            date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
            date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
        else:
            cur.execute("SELECT DISTINCT DATE(transactionDate) AS transactionDate FROM mpesa_transactions")
            date = pd.DataFrame(cur.fetchall())
            date['transactionDate'] = pd.to_datetime(date['transactionDate'], errors='coerce')
            date['transactionDate'] = date['transactionDate'].dt.strftime('%Y-%m-%d')

        response = date.to_json(orient='records')
        return response, 200



    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()


# get fullname and filename from mpesa_transactions summary
@app.route('/get_fullname', methods=['GET'])
def name():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        fileName = request.args.get('fileName')
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')
        if fileName or startDate or endDate:
            if fileName and startDate and endDate:
                cur.execute(
                    f"SELECT DISTINCT FileName, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName = {fileName} and DATE(transactionDate) BETWEEN {startDate} AND {endDate}")
                date = pd.DataFrame(cur.fetchall())
                date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                response = date.to_json(orient='records')
                return response, 200
            if startDate and endDate:
                cur.execute(
                    f"SELECT DISTINCT DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions WHERE DATE(transactionDate) BETWEEN {startDate} AND {endDate}")
                date = pd.DataFrame(cur.fetchall())
                date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                response = date.to_json(orient='records')
                return response, 200
            elif endDate:
                cur.execute(
                    f"SELECT DISTINCT DATE(transactionDate) AS endDate FROM mpesa_transactions WHERE DATE(transactionDate) <= {endDate}")
                date = pd.DataFrame(cur.fetchall())
                date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                response = date.to_json(orient='records')
                return response, 200
            elif startDate:
                cur.execute(
                    f"SELECT DISTINCT DATE(transactionDate) AS startDate FROM mpesa_transactions WHERE DATE(transactionDate) >= {startDate}")
                date = pd.DataFrame(cur.fetchall())
                date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                response = date.to_json(orient='records')
                return response, 200
            else:
                cur.execute("SELECT DISTINCT fullNames, fileName FROM mpesa_transactions_summary WHERE fileName = %s",
                            fileName)
                mpesa = cur.fetchall()
                response = jsonify(mpesa)
                response.status_code = 200
                return response


        else:
            cur.execute("SELECT DISTINCT fullNames, fileName FROM mpesa_transactions_summary")
            mpesa = cur.fetchall()
            response = jsonify(mpesa)
            response.status_code = 200
            return response


    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

    # get amount against transactiontype


@app.route('/get_transtype', methods=['GET'])
def transaction_amount():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        fileName = request.args.get('fileName')
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')
        if fileName or startDate or endDate:
            if fileName and startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT  fileName, SUM(ABS(amount)) AS amount, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName = {fileName} and DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY fileName, transactionType")
                    date = pd.DataFrame(cur.fetchall())
                    date.drop_duplicates(inplace=True)
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                except Exception:
                    return "Record not found!!!"
                response = date.to_json(orient='records')
                return response, 200
            elif fileName and endDate:
                try:
                    cur.execute(
                        f"SELECT fileName, transactionType, SUM(ABS(amount)) AS amount ,DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE DATE(transactionDate) <= {endDate} AND fileName = {fileName} GROUP BY fileName, transactionType")
                    date = pd.DataFrame(cur.fetchall())
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                except Exception:
                    return "Record not found!!!"
                response = date.to_json(orient='records')
                return response, 200

            elif fileName and startDate:
                try:
                    cur.execute(
                        f"SELECT fileName, transactionType, SUM(ABS(amount)) AS amount ,DATE(transactionDate) AS startDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE DATE(transactionDate) >= {startDate} AND fileName = {fileName} GROUP BY fileName, transactionType")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                except Exception:
                    return "Record not found!!!"
                response = date.to_json(orient='records')
                return response, 200

            elif startDate and endDate:
                cur.execute(
                    f"SELECT transactionType, SUM(ABS(amount)) AS amount ,DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY transactionType")
                date = pd.DataFrame(cur.fetchall())
                date.drop_duplicates(inplace=True)
                date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                response = date.to_json(orient='records')
                return response, 200
            elif endDate:
                cur.execute(
                    f"SELECT DISTINCT transactionType, SUM(ABS(amount)) AS amount ,DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE DATE(transactionDate) <= {endDate} GROUP BY transactionType")
                date = pd.DataFrame(cur.fetchall())
                date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                response = date.to_json(orient='records')
                return response, 200
            elif startDate:
                cur.execute(
                    f"SELECT DISTINCT transactionType, SUM(ABS(amount)) AS amount , DATE(transactionDate) AS startDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE DATE(transactionDate) >= {startDate} GROUP BY transactionType")
                date = pd.DataFrame(cur.fetchall())
                date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                response = date.to_json(orient='records')
                return response, 200
            else:
                cur.execute(
                    "SELECT fileName, transactionType, SUM(ABS(amount)) AS amount FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName=%s GROUP BY fileName, transactionType",
                    fileName)
                trans_amount = cur.fetchall()
                response = jsonify(trans_amount)
                response.status_code = 200
                return response

        else:
            cur.execute(
                "SELECT transactionType, sum(abs(amount)) AS amount FROM mpesa_transactions GROUP BY transactionType")
            trans_amount = cur.fetchall()
            response = jsonify(trans_amount)
            response.status_code = 200
            return response


    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

    # get total in vs total out per month


@app.route('/get_totals', methods=['GET'])
def total_amount():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        fileName = request.args.get('fileName')
        totaltype = request.args.get('transactionType')
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')
        if fileName or startDate or endDate:
            if fileName and startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT fileName, summaryId, transactionType, CONCAT(MONTHNAME(transactionDate),' ',YEAR(transactionDate)) AS Month_Year, SUM(ABS(amount)) AS Total ,DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate, YEAR(transactionDate) AS Year, MONTH(transactionDate) AS Month FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE transactionType= {totaltype} AND fileName = {fileName} and DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY fileName, summaryId, transactionType, CONCAT(MONTHNAME(transactionDate),' ',YEAR(transactionDate)) ORDER BY YEAR(transactionDate), MONTH(transactionDate)")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                    date.drop_duplicates(subset=['fileName', 'transactionType', 'Month_Year', 'Month', 'Year'],
                                         keep='first', inplace=True)
                    date = date.groupby(['transactionType', 'Month_Year', 'Month', 'Year'], as_index=False)[
                        'Total'].sum()
                    date = date.sort_values(by=['Year', 'Month'], ascending=True)

                except Exception:
                    return "Record not found!!!"
                response = date.to_json(orient='records')
                return response, 200


            elif startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT fileName, summaryId,  transactionType, CONCAT(MONTHNAME(transactionDate),' ',YEAR(transactionDate)) AS Month_Year, SUM(ABS(amount)) AS Total ,DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate, YEAR(transactionDate) AS Year, MONTH(transactionDate) AS Month FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE transactionType= {totaltype} AND DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY fileName, summaryId, transactionType, CONCAT(MONTHNAME(transactionDate),' ',YEAR(transactionDate)) ORDER BY YEAR(transactionDate), MONTH(transactionDate)")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                    date.drop_duplicates(subset=['fileName', 'transactionType', 'Month_Year', 'Month', 'Year'],
                                         keep='first', inplace=True)
                    date = date.groupby(['transactionType', 'Month_Year', 'Month', 'Year'], as_index=False)[
                        'Total'].sum()
                    date = date.sort_values(by=['Year', 'Month'], ascending=True)

                except Exception:
                    return "Records not found!!!"
                response = date.to_json(orient='records')
                return response, 200
            else:
                cur.execute(
                    "SELECT fileName, summaryId, transactionType, CONCAT(MONTHNAME(transactionDate),' ',YEAR(transactionDate)) AS Month_Year, SUM(ABS(amount)) AS Total, YEAR(transactionDate) AS Year, MONTH(transactionDate) AS Month FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName=%s and transactionType=%s GROUP BY fileName, summaryId,  transactionType, CONCAT(MONTHNAME(transactionDate),' ',YEAR(transactionDate)) ORDER BY YEAR(transactionDate), MONTH(transactionDate);",
                    (fileName, totaltype))
                totals = pd.DataFrame(cur.fetchall())
                totals.drop_duplicates(subset=['fileName', 'transactionType', 'Month_Year', 'Month', 'Year'],
                                       inplace=True)
                totals = totals.groupby(['transactionType', 'Month_Year', 'Month', 'Year'], as_index=False)[
                    'Total'].sum()
                totals = totals.sort_values(by=['Year', 'Month'], ascending=True)
                response = totals.to_json(orient='records')
                return response, 200
        else:
            cur.execute(
                "SELECT fileName, summaryId, transactionType, CONCAT(MONTHNAME(transactionDate),' ',YEAR(transactionDate)) AS Month_Year, SUM(ABS(amount)) AS Total, YEAR(transactionDate) AS Year, MONTH(transactionDate) AS Month FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE transactionType=%s GROUP BY fileName, summaryId, transactionType, CONCAT(MONTHNAME(transactionDate),' ',YEAR(transactionDate)) ORDER BY YEAR(transactionDate), MONTH(transactionDate);",
                totaltype)
            totals = pd.DataFrame(cur.fetchall())
            totals.drop_duplicates(subset=['fileName', 'transactionType', 'Month_Year', 'Month', 'Year'], inplace=True)
            totals = totals.groupby(['transactionType', 'Month_Year', 'Month', 'Year'], as_index=False)['Total'].sum()
            totals = totals.sort_values(by=['Year', 'Month'], ascending=True)
            response = totals.to_json(orient='records')
            return response, 200

    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

    # get volume of transactions


@app.route('/get_volumes', methods=['GET'])
def transaction_volumes():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        fileName = request.args.get('fileName')
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')
        if fileName or startDate or endDate:
            if fileName and startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT fileName, summaryId, agentDepositPaidIn AS agentDeposit, agentWithdrawalPaidOut AS agentWithdrawal, buyGoodsPaidOut AS buyGoods, othersPaidIn AS othersPaidIn, othersPaidOut AS othersPaidOut, payBillPaidOut as paybill, receiveMoneyIn AS receiveMoney, sendMoneyOut  AS sendMoney, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName = {fileName} and DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY fileName, summaryId")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                    date.drop_duplicates(
                        subset=['fileName', 'agentDeposit', 'agentWithdrawal', 'buyGoods', 'othersPaidIn',
                                'othersPaidOut', 'paybill', 'receiveMoney', 'sendMoney'], keep='first', inplace=True)
                except Exception:
                    return "Record not found!!!"
                response = date.to_json(orient='records')
                return response, 200

            elif startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT fileName, summaryId, agentDepositPaidIn AS agentDeposit, agentWithdrawalPaidOut AS agentWithdrawal, buyGoodsPaidOut AS buyGoods, othersPaidIn AS othersPaidIn, othersPaidOut AS othersPaidOut, payBillPaidOut as paybill, receiveMoneyIn AS receiveMoney, sendMoneyOut  AS sendMoney, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY fileName, summaryId")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                    date.drop_duplicates(
                        subset=['fileName', 'agentDeposit', 'agentWithdrawal', 'buyGoods', 'othersPaidIn',
                                'othersPaidOut', 'paybill', 'receiveMoney', 'sendMoney'], keep='first', inplace=True)
                    date = date[
                        ['agentDeposit', 'agentWithdrawal', 'buyGoods', 'othersPaidIn', 'othersPaidOut', 'paybill',
                         'receiveMoney', 'sendMoney']].sum()
                except Exception:
                    return "Records not found!!!"
                response = date.to_json(orient='records')
                return response, 200
            else:
                cur.execute(
                    'SELECT fileName, summaryId,  agentDepositPaidIn AS agentDeposit, agentWithdrawalPaidOut AS agentWithdrawal, buyGoodsPaidOut AS buyGoods, othersPaidIn AS othersPaidIn, othersPaidOut AS othersPaidOut, payBillPaidOut as paybill, receiveMoneyIn AS receiveMoney, sendMoneyOut  AS sendMoney, FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName=%s GROUP BY fileName, summaryId',
                    fileName)
                volumes = pd.DataFrame(cur.fetchall())
                volumes.drop_duplicates(
                    subset=['fileName', 'agentDeposit', 'agentWithdrawal', 'buyGoods', 'othersPaidIn', 'othersPaidOut',
                            'paybill', 'receiveMoney', 'sendMoney'], keep='first', inplace=True)
                volumes = volumes.groupby('fileName', as_index=False)[
                    'agentDeposit', 'agentWithdrawal', 'buyGoods', 'othersPaidIn', 'othersPaidOut', 'paybill', 'receiveMoney', 'sendMoney'].sum()
                response = volumes.to_json(orient='records')
                return response, 200
        else:
            cur.execute(
                'SELECT fileName, summaryId,  agentDepositPaidIn AS agentDeposit, agentWithdrawalPaidOut AS agentWithdrawal, buyGoodsPaidOut AS buyGoods, othersPaidIn AS othersPaidIn, othersPaidOut AS othersPaidOut, payBillPaidOut as paybill, receiveMoneyIn AS receiveMoney, sendMoneyOut  AS sendMoney FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id GROUP BY fileName, summaryId')
            volumes = pd.DataFrame(cur.fetchall())
            volumes.drop_duplicates(
                subset=['fileName', 'agentDeposit', 'agentWithdrawal', 'buyGoods', 'othersPaidIn', 'othersPaidOut',
                        'paybill', 'receiveMoney', 'sendMoney'], keep='first', inplace=True)
            volumes = volumes[
                ['agentDeposit', 'agentWithdrawal', 'buyGoods', 'othersPaidIn', 'othersPaidOut', 'paybill',
                 'receiveMoney', 'sendMoney']].sum()
            response = volumes.to_json(orient='records')
            return response, 200
    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

    # get transactions per month


@app.route('/transactionspermonth', methods=['GET'])
def monthly_transactions():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        fileName = request.args.get('fileName')
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')
        if fileName or startDate or endDate:
            if fileName and startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT  fileName, summaryId, transactionType ,year(transactionDate) as year, monthname(transactionDate) as Month, month(transactionDate) AS Month_No, count(summaryId) as Total, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate, sum(abs(amount)) AS TotalAmount FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName = {fileName} and DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY summaryId, fileName, transactionType ,year(transactionDate), monthname(transactionDate) ORDER BY year(transactionDate), month(transactionDate)")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                    date.drop_duplicates(
                        subset=['fileName', 'transactionType', 'year', 'Month', 'Total', 'startDate', 'endDate',
                                'TotalAmount'], keep="first", inplace=True)
                    date = date.groupby(['fileName', 'transactionType', 'year', 'Month', 'Month_No'], as_index=False)[
                        'Total', 'TotalAmount'].sum()
                    date = date.sort_values(by=['year', 'Month_No'], ascending=True)

                except Exception:
                    return "Record not found!!!"
                response = date.to_json(orient='records')
            elif startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT fileName, transactionType ,summaryId, year(transactionDate) as year, monthname(transactionDate) as Month, month(transactionDate) as Month_No, count(summaryId) as Total, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate, sum(abs(amount)) AS TotalAmount FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY transactionType ,fileName, summaryId, year(transactionDate), monthname(transactionDate) ORDER BY year(transactionDate), month(transactionDate)")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                    date.drop_duplicates(
                        subset=['fileName', 'transactionType', 'year', 'Month', 'Total', 'TotalAmount'], keep="first",
                        inplace=True)
                    date = date.groupby(['year', 'Month', 'transactionType', 'Month_No'], as_index=False)[
                        'Total', 'TotalAmount'].sum()
                    date = date.sort_values(by=['year', 'Month_No'], ascending=True)
                except Exception:
                    return "Records not found!!!"
                response = date.to_json(orient='records')
            else:
                cur.execute(
                    'SELECT summaryId, fileName, transactionType , year(transactionDate) as year, monthname(transactionDate) as Month, count(summaryId) as Total,month(transactionDate) as Month_No, sum(abs(amount)) AS TotalAmount FROM  mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId  = mpesa_transactions_summary.id WHERE fileName=%s GROUP BY summaryId, transactionType ,fileName, year(transactionDate), monthname(transactionDate) ORDER BY summaryId, year(transactionDate), month(transactionDate)',
                    fileName)
                transactions_month = pd.DataFrame(cur.fetchall())
                transactions_monthly = transactions_month.drop_duplicates(
                    subset=['fileName', 'year', 'Month', 'Total', 'TotalAmount'], keep="first")
                transactions_monthly = \
                    transactions_monthly.groupby(['fileName', 'transactionType', 'year', 'Month', 'Month_No'],
                                                 as_index=False)['Total', 'TotalAmount'].sum()
                transactions_monthly = transactions_monthly.sort_values(by=['year', 'Month_No'], ascending=True)
                response = transactions_monthly.to_json(orient='records')
            return response, 200
        else:
            cur.execute(
                'SELECT fileName, transactionType ,summaryId, year(transactionDate) as year, monthname(transactionDate) as Month, month(transactionDate) as Month_No, count(summaryId) as Total, sum(abs(amount)) AS TotalAmount FROM  mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId  = mpesa_transactions_summary.id GROUP BY fileName, transactionType ,summaryId, year(transactionDate), monthname(transactionDate) ORDER BY year(transactionDate), month(transactionDate)')
            transactions_month = pd.DataFrame(cur.fetchall())
            transactions_monthly = transactions_month.drop_duplicates(
                subset=['fileName', 'year', 'Month', 'Total', 'TotalAmount'], keep="first")
            transactions_monthly = \
                transactions_monthly.groupby(['year', 'transactionType', 'Month', 'Month_No'], as_index=False)[
                    ['Total', 'TotalAmount']].sum()
            transactions_monthly = transactions_monthly.sort_values(by=['year', 'Month_No'], ascending=True)
            response = transactions_monthly.to_json(orient='records')
            return response, 200
    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()


# get top transactions
@app.route('/get_toptransactions', methods=['GET'])
def top_transactions():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        fileName = request.args.get('fileName')
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')
        if fileName or startDate or endDate:
            if fileName and startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT fileName, summaryId, description, COUNT(description) AS no_transactions, SUM(abs(amount)) as Total, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName = {fileName} and DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY fileName, summaryId, description")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                    date.drop_duplicates(['fileName', 'description', 'no_transactions', 'Total'], keep='first',
                                         inplace=True)
                    date = date.groupby(['fileName', 'description'], as_index=False)['no_transactions', 'Total'].sum()
                    date = date.sort_values(by=['no_transactions'], ascending=False).head(15)
                except Exception:
                    return "Record not found!!!"
                response = date.to_json(orient='records')
                return response, 200
            elif startDate and endDate:
                try:
                    cur.execute(
                        f"SELECT fileName, summaryId,  description, COUNT(description) AS no_transactions, SUM(abs(amount)) as Total, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE DATE(transactionDate) BETWEEN {startDate} AND {endDate} GROUP BY fileName, summaryId, description ORDER BY COUNT(description) DESC")
                    date = pd.DataFrame(cur.fetchall())
                    date['startDate'] = pd.to_datetime(date['startDate'], errors='coerce')
                    date['startDate'] = date['startDate'].dt.strftime('%Y-%m-%d')
                    date['endDate'] = pd.to_datetime(date['endDate'], errors='coerce')
                    date['endDate'] = date['endDate'].dt.strftime('%Y-%m-%d')
                    date.drop_duplicates(['fileName', 'description', 'no_transactions', 'Total'], keep='first',
                                         inplace=True)
                    date = date.groupby(['description'], as_index=False)['no_transactions', 'Total'].sum()
                    date = date.sort_values(by='no_transactions', ascending=False).head(15)
                except Exception:
                    return "Records not found!!!"
                response = date.to_json(orient='records')
                return response, 200

            else:
                cur.execute(
                    'SELECT fileName, summaryId, description, payBillName, COUNT(description) AS no_transactions, SUM(abs(amount)) as Total FROM  mpesa_transactions_summary JOIN mpesa_transactions ON mpesa_transactions_summary.id  = mpesa_transactions.summaryId WHERE fileName=%s GROUP BY fileName, summaryId, description, payBillName ORDER BY fileName, COUNT(description) DESC',
                    fileName)
                toptrans = pd.DataFrame(cur.fetchall())
                toptrans.drop_duplicates(['fileName', 'description', 'no_transactions', 'Total'], keep='first',
                                         inplace=True)
                toptrans = toptrans.groupby(['fileName', 'description'], as_index=False)[
                    'no_transactions', 'Total'].sum()
                toptrans = toptrans.sort_values(by='no_transactions', ascending=False).head(15)
                response = toptrans.to_json(orient='records')
                return response, 200
        else:
            cur.execute(
                'SELECT fileName, summaryId, description, COUNT(description) AS no_transactions, SUM(abs(amount)) as Total FROM  mpesa_transactions_summary JOIN mpesa_transactions ON mpesa_transactions_summary.id  = mpesa_transactions.summaryId GROUP BY fileName, summaryId, description ORDER BY COUNT(description) DESC')
            toptrans = pd.DataFrame(cur.fetchall())
            toptrans.drop_duplicates(['fileName', 'description', 'no_transactions', 'Total'], keep='first',
                                     inplace=True)
            toptrans = toptrans.groupby(['description'], as_index=False)[['no_transactions', 'Total']].sum()
            toptrans = toptrans.sort_values(by='no_transactions', ascending=False).head(15)
            response = toptrans.to_json(orient='records')
            return response, 200
    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()


# get statement period
@app.route('/get_statementperiod', methods=['GET'])
def statement_period():
    try:
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        fileName = request.args.get('fileName')
        if fileName:
            cur.execute(
                'SELECT fileName, fullNames, statementPeriod  FROM  mpesa_transactions_summary WHERE fileName=%s GROUP BY fileName',
                fileName)
            statementperiod = cur.fetchall()
            # add dateOfStatement AS "Date of Statement"
            response = jsonify(statementperiod)
            response.status_code = 200
            return response
        else:
            cur.execute(
                'SELECT fileName, fullNames, statementPeriod FROM  mpesa_transactions_summary GROUP BY fileName')
            statementperiod = cur.fetchall()
            response = jsonify(statementperiod)
            response.status_code = 200
            return response

    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

    # select mpesa_transactions for paybills classification


@app.route('/get_paybill', methods=['GET'])
def paybillclass():
    conn = mysql.connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    fileName = request.args.get('fileName')
    startDate = request.args.get('startDate')
    endDate = request.args.get('endDate')
    if fileName or startDate or endDate:
        if fileName and startDate and endDate:
            try:
                cur.execute(
                    f"SELECT fileName, summaryId, description, paybillname, sum(abs(amount)) AS amount, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId = mpesa_transactions_summary.id WHERE fileName = {fileName} and DATE(transactionDate) BETWEEN {startDate} AND {endDate} AND transactionType  = \'WITHDRAWN\' AND paybillname IS NOT NULL GROUP BY fileName, summaryId, description, paybillname")
                paybills = pd.DataFrame(cur.fetchall())
                paybills['endDate'] = pd.to_datetime(paybills['endDate'], errors='coerce')
                paybills['endDate'] = paybills['endDate'].dt.strftime('%Y-%m-%d')
                paybills.drop_duplicates(subset=['fileName', 'description', 'paybillname', 'amount'], keep='first',
                                         inplace=True)
                cur.close()
                conn.close()

                # convert to lower case
                paybills['paybillname'] = paybills['paybillname'].apply(lambda x: x.lower())
                paybills['description'] = paybills['description'].apply(lambda x: x.lower())

                # transform to tf-idf format
                paybillname = paybill_vect.transform(paybills['paybillname'])
                description = desc_vect.transform(paybills['description'])

                # form stack sparse matrix
                test = scipy.sparse.hstack((paybillname, description)).tocsr()

                # make prediction
                values = model.predict(test)

                # append values into dataframe
                paybills['paybillclassid'] = values
                paybills.loc[paybills['paybillclassid'] == 0, ['paybillclassid']] = 'Withdrawal/Deposits'
                paybills.loc[paybills['paybillclassid'] == 1, ['paybillclassid']] = 'Lender'
                paybills.loc[paybills['paybillclassid'] == 2, ['paybillclassid']] = 'Entertainment'
                paybills.loc[paybills['paybillclassid'] == 3, ['paybillclassid']] = 'Utilities'
                paybills.loc[paybills['paybillclassid'] == 4, ['paybillclassid']] = 'Other Organizations'
                paybills.loc[paybills['paybillclassid'] == 5, ['paybillclassid']] = 'Financial'
                paybills.loc[paybills['paybillclassid'] == 6, ['paybillclassid']] = 'Travel'
                paybills.loc[paybills['paybillclassid'] == 7, ['paybillclassid']] = 'School fees'
                paybills.loc[paybills['paybillclassid'] == 8, ['paybillclassid']] = 'Betting'

                # convert paybills to json format
                paybill_cats = paybills.groupby(['fileName', 'paybillclassid'], as_index=False)['amount'].sum()
            except Exception:
                return "Records not found!!!"
            response = paybill_cats.to_json(orient='records')
            return response, 200

        elif startDate and endDate:
            try:
                cur.execute(
                    f"SELECT fileName, summaryId, description, paybillname, sum(abs(amount)) AS amount, DATE(transactionDate) AS startDate, DATE(transactionDate) AS endDate FROM  mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId=mpesa_transactions_summary.id WHERE DATE(transactionDate) BETWEEN {startDate} AND {endDate} AND transactionType  = \'WITHDRAWN\' AND paybillname IS NOT NULL GROUP BY fileName, summaryId, description, paybillname")
                paybills = pd.DataFrame(cur.fetchall())
                paybills['endDate'] = pd.to_datetime(paybills['endDate'], errors='coerce')
                paybills['endDate'] = paybills['endDate'].dt.strftime('%Y-%m-%d')
                paybills.drop_duplicates(subset=['fileName', 'description', 'paybillname', 'amount'], keep='first',
                                         inplace=True)
                cur.close()
                conn.close()

                # convert to lower case
                paybills['paybillname'] = paybills['paybillname'].apply(lambda x: x.lower())
                paybills['description'] = paybills['description'].apply(lambda x: x.lower())

                # transform to tfidf format
                paybillname = paybill_vect.transform(paybills['paybillname'])
                description = desc_vect.transform(paybills['description'])

                # form stack sparse matrix
                test = scipy.sparse.hstack((paybillname, description)).tocsr()

                # make prediction
                values = model.predict(test)

                # append values into dataframe
                paybills['paybillclassid'] = values
                paybills.loc[paybills['paybillclassid'] == 0, ['paybillclassid']] = 'W/D'
                paybills.loc[paybills['paybillclassid'] == 1, ['paybillclassid']] = 'Lender'
                paybills.loc[paybills['paybillclassid'] == 2, ['paybillclassid']] = 'Entertainment'
                paybills.loc[paybills['paybillclassid'] == 3, ['paybillclassid']] = 'Utilities'
                paybills.loc[paybills['paybillclassid'] == 4, ['paybillclassid']] = 'Other'
                paybills.loc[paybills['paybillclassid'] == 5, ['paybillclassid']] = 'Financial'
                paybills.loc[paybills['paybillclassid'] == 6, ['paybillclassid']] = 'Travel'
                paybills.loc[paybills['paybillclassid'] == 7, ['paybillclassid']] = 'School fees'
                paybills.loc[paybills['paybillclassid'] == 8, ['paybillclassid']] = 'Betting'

                # convert paybills to json format
                paybill_cats = paybills.groupby(['paybillclassid'], as_index=False)['amount'].sum()
            except Exception:
                return "Records not found!!!"
            response = paybill_cats.to_json(orient='records')
            return response, 200

        else:
            cur.execute(
                'SELECT fileName, summaryId, description, paybillname, sum(abs(amount)) FROM  mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId=mpesa_transactions_summary.id WHERE transactionType = "WITHDRAWN" AND fileName=%s AND paybillname IS NOT NULL GROUP BY fileName, summaryId,  description, paybillname',
                fileName)
            paybills = pd.DataFrame(cur.fetchall())
            paybills.drop_duplicates(subset=['fileName', 'description', 'paybillname', 'amount'], keep='first',
                                     inplace=True)
            cur.close()
            conn.close()

        # convert to lower case
        paybills['paybillname'] = paybills['paybillname'].apply(lambda x: x.lower())
        paybills['description'] = paybills['description'].apply(lambda x: x.lower())

        # transform to tfidf format
        paybillname = paybill_vect.transform(paybills['paybillname'])
        description = desc_vect.transform(paybills['description'])

        # form stack sparse matrix
        test = scipy.sparse.hstack((paybillname, description)).tocsr()

        # make prediction
        values = model.predict(test)

        # append values into dataframe
        paybills['paybillclassid'] = values
        paybills.loc[paybills['paybillclassid'] == 0, ['paybillclassid']] = 'Withdrawal/Deposits'
        paybills.loc[paybills['paybillclassid'] == 1, ['paybillclassid']] = 'Lender'
        paybills.loc[paybills['paybillclassid'] == 2, ['paybillclassid']] = 'Entertainment'
        paybills.loc[paybills['paybillclassid'] == 3, ['paybillclassid']] = 'Utilities'
        paybills.loc[paybills['paybillclassid'] == 4, ['paybillclassid']] = 'Other Organizations'
        paybills.loc[paybills['paybillclassid'] == 5, ['paybillclassid']] = 'Financial'
        paybills.loc[paybills['paybillclassid'] == 6, ['paybillclassid']] = 'Travel'
        paybills.loc[paybills['paybillclassid'] == 7, ['paybillclassid']] = 'School fees'
        paybills.loc[paybills['paybillclassid'] == 8, ['paybillclassid']] = 'Betting'

        # convert paybills to json format
        paybill_cats = paybills.groupby(['fileName', 'paybillclassid'], as_index=False)['amount'].sum()
        paybill_cats.to_json(r'paybill_classes_filename.json', orient='table')
        with open('paybill_classes_filename.json', 'r') as f:
            # return json.load(f), 200
            # {'paybill_categories': json.load(f)}
            return {'paybill_categories': json.load(f)}, 200

    else:
        cur.execute(
            'SELECT fileName, summaryId, description, paybillname, sum(abs(amount)) as amount FROM  mpesa_transactions INNER JOIN mpesa_transactions_summary ON mpesa_transactions.summaryId=mpesa_transactions_summary.id WHERE transactionType = "WITHDRAWN" AND paybillname IS NOT NULL GROUP BY fileName, summaryId, description, paybillname')
        paybills = pd.DataFrame(cur.fetchall())
        paybills.drop_duplicates(subset=['fileName', 'description', 'paybillname', 'amount'], keep='first',
                                 inplace=True)
        cur.close()
        conn.close()

        # convert to lower case
        paybills['paybillname'] = paybills['paybillname'].apply(lambda x: x.lower())
        paybills['description'] = paybills['description'].apply(lambda x: x.lower())

        # transform to tfidf format
        paybillname = paybill_vect.transform(paybills['paybillname'])
        description = desc_vect.transform(paybills['description'])

        # form stack sparse matrix
        test = scipy.sparse.hstack((paybillname, description)).tocsr()

        # make prediction
        values = model.predict(test)

        # append values into dataframe
        paybills['paybillclassid'] = values
        paybills.loc[paybills['paybillclassid'] == 0, ['paybillclassid']] = 'Withdrawal/Deposits'
        paybills.loc[paybills['paybillclassid'] == 1, ['paybillclassid']] = 'Lender'
        paybills.loc[paybills['paybillclassid'] == 2, ['paybillclassid']] = 'Entertainment'
        paybills.loc[paybills['paybillclassid'] == 3, ['paybillclassid']] = 'Utilities'
        paybills.loc[paybills['paybillclassid'] == 4, ['paybillclassid']] = 'Other Organizations'
        paybills.loc[paybills['paybillclassid'] == 5, ['paybillclassid']] = 'Financial'
        paybills.loc[paybills['paybillclassid'] == 6, ['paybillclassid']] = 'Travel'
        paybills.loc[paybills['paybillclassid'] == 7, ['paybillclassid']] = 'School fees'
        paybills.loc[paybills['paybillclassid'] == 8, ['paybillclassid']] = 'Betting'

        # convert paybills to json format
        paybill_cats = paybills.groupby(['paybillclassid'], as_index=False)['amount'].sum()
        paybill_cats.to_json(r'paybill_classes.json', orient='table')
        with open('paybill_classes.json', 'r') as f:
            # return json.load(f), 200
            # {'paybill_categories': json.load(f)}
            return {'paybill_categories': json.load(f)}, 200


#################################################### ERROR HANDLER ###########################################################
@app.errorhandler(404)
def error_message(error=None):
    message = {'status': 404,
               'message': f'Record not found {request.url}'
               }
    response = jsonify(message)
    response.status_code = 404
    return response


if __name__ == '__main__':
    app.run()
