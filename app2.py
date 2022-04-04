# Library Imports
import json     
import psycopg2 
from flask import Flask 
from flask import request 
from flask_restful import Api
from sqlalchemy import Column, String, Integer, Date, BOOLEAN, and_, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from flask import jsonify
import os
from datetime import date
import logging as log
log.basicConfig(level=log.DEBUG)

app = Flask(__name__)
api = Api(app)



Base = declarative_base()


database_url = "postgresql://postgres:1234@localhost:5432/postgres"


engine = create_engine(database_url, echo=True, poolclass=NullPool)


conn = engine.connect()


Session = sessionmaker(bind=engine)


session = Session()


class ProductEnquiry(Base):
    """ Product enquiry form model which has all details -table names & columns"""
    __tablename__ = "productenquiry"
    __table_args__ = {'schema':'public'}
    createdDate = Column("createddate", String)
    dealerCode = Column("dealercode", String)
    customerName = Column("customername", String)
    mobileNumber = Column("mobilenumber", Integer, primary_key=True)
    emailId = Column("emailid", String)
    vehicleModel = Column("vehiclemodel", String)
    state = Column("state", String)
    district = Column("distric", String)
    city = Column("city", String)
    existingVehicle = Column("exstingvehicle", String)
    wantTestDrive = Column("wanttestdrive", BOOLEAN)
    dealerState = Column("dealerstate", String)
    dealerTown = Column("dealertown", String)
    dealer = Column("dealer", String)
    briefAboutEnquery = Column("briefaboutenquery", String)
    expectedDateOfPurchase = Column("expecteddateofpurchase", String)
    gender = Column("gender", String)
    age = Column("age", Integer)
    occupation = Column("occupation", String)
    intendedUsage = Column("intendedusage", String)


class Dealer(Base):
    """ Dealer model which has all details -table names & columns"""
    __tablename__ = "dealer"
    dealerName = Column("dealername", String, primary_key=True)
    dealerCode = Column("dealercode", String)
    

@app.route('/en-in/reach-us/product-enquiry1', methods=['GET'])
def fetchTodaysLeadsInfo1():
    """Returns the all leads info"""
    # Select * from public.productenquiry;
    product_result = session.query(ProductEnquiry).all()
    # [<__main__.ProductEnquiry object at 0x03632280>, <__main__.ProductEnquiry object at 0x03632250>]
    product_result2 = [item.__dict__ for item in product_result]
    return str(product_result2)


@app.route('/en-in/reach-us/product-enquiry', methods=['GET'])
def fetchTodaysLeadsInfo():
    """Returns the all leads info"""
    # Select * from public.productenquiry;
    product_result = session.query(ProductEnquiry).filter(ProductEnquiry.customerName=='Jhon').all()
    # [<__main__.ProductEnquiry object at 0x03632280>, <__main__.ProductEnquiry object at 0x03632250>]
    product_result2 = [item.__dict__ for item in product_result]
    return str(product_result2)



@app.route('/get/single/record',methods=['GET'])
def singleRecord():
    product_result= session.query(ProductEnquiry).filter(ProductEnquiry.dealerCode=='HYD001').all()
    product_result1 = [item.__dict__ for item in product_result]
    return str(product_result1)


@app.route('/starts/with/record', methods=['GET'])
def getStartsRecord():
    
    mobile = request.args.get('mobile')
    result = session.query(ProductEnquiry).filter(ProductEnquiry.mobilenumber.like(mobile+'%')).all()
    print(type(result))
    result2 = [item.__dict__ for item in result]
    return jsonify(product_result_dict)

@app.route('/ends_record', methods=['GET'])
def endsRecord():
    """Returns the leads info for current date.."""
    log.info("endsRecord : endsRecord")
    product_result = []
    mobile = request.args.get('mobile')
    name = request.args.get('name')
    log.debug("mobile is {}".format(mobile))
    log.debug("name is {}".format(name))

    try:
        product_result = session.query(ProductEnquiry).filter(
            ProductEnquiry.mobileNumber.like('%' + mobile), ProductEnquiry.customerName.like('%' + name)).all()
        log.debug("product_result is {}".format(product_result))

    except Exception as err:
        session.rollback()
        log.error("Error occurred while ProductEnquiry table sql transaction is {}".format(err))
    finally:
        session.close()
        product_result_dict = [item.__dict__ for item in product_result]
        log.debug("product_result_dict is {}".format(product_result_dict))
        for item in product_result_dict:
            del item['_sa_instance_state']
            log.info("starts_record : Ended")
            return jsonify(product_result_dict)


@app.route('/fetch-todays-leads', methods=['GET'])
def fetchTodaysLeads():
    """Returns the leads info for current date.."""
    log.info("fetchTodaysLeads : Started")
    dealer_result = []
    product_result = []
    dealercode = request.args.get('dealer_code')
    log.debug("dealer_code is {}".format(dealercode))

    try:
        dealer_result = session.query(Dealer).filter(Dealer.dealerCode == dealercode).all()
        log.debug("dealer_result is {}".format(dealer_result))

    except Exception as err:
        log.error("Error occured while dealer table sql transaction is {}".format(err))
        session.rollback()

    if dealer_result:
        try:
            
           product_result = session.query(ProductEnquiry).filter(ProductEnquiry.createdDate==str(currentdate),ProductEnquiry.dealerCode==request.args.get('dealer_code')).all()
           log.debug("product_result is {}".format(product_result))
        except Exception as err:
            session.rollback()
            log.error("Error occured while ProductEnquiry table sql transaction is {}".format(err))
        finally:
            session.close()
            product_result_dict = [item.__dict__ for item in product_result]
            log.debug("product_result_dict is {}".format(product_result_dict))
        for item in product_result_dict:
            del item['_sa_instance_state']
            log.info("fetchTodaysLeads : Ended")
            return jsonify(product_result_dict)
    else:
        log.info("fetchTodaysLeads : Ended")
        return "Unauthorized access"

app.run(debug=False)
