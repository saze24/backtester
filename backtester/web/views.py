import multiprocessing as mp
import os
import psutil
import sys
import webbrowser
from datetime import datetime
from flask import Blueprint, render_template, flash, redirect, url_for, request, session
from . import backtester


views = Blueprint('views', __name__)

@views.route('/', methods=['GET', 'POST'])
def run_tests():
    # Multiprocessing is necessary to complete the tests in a timely fashion
    # Use the variable ranges submitted to start testing through backtester.run_test()

    try:
        db_path = os.getcwd() + '\\web\\database\\backtester_database.db'
        data_import_check = False

        # Check if the database doesn't exist
        if not os.path.exists(db_path):
            # Check if the csv to make sure there is no missing or non-conforming data
            data_import_check = backtester.data_import_check()
            if data_import_check[0] == 'Data Import Error':
                flash('Data Import Error', category='error')
                flash(data_import_check[1][0], category='error')
                flash(data_import_check[1][1], category='error')
                return render_template('run_tests.html')
        else:
            # There is a database so saved_results.html can appear on nav bar
            session['saved_results_exist'] = True

        if request.method == 'POST':
            test_name = request.form.get('test_name')
            fast_ma_low = int(request.form.get('fast_ma_low'))
            fast_ma_high = int(request.form.get('fast_ma_high'))
            slow_ma_low = int(request.form.get('slow_ma_low'))
            slow_ma_high = int(request.form.get('slow_ma_high'))
            stop_loss_low = int(request.form.get('stop_loss_low'))
            stop_loss_high = int(request.form.get('stop_loss_high'))
            take_profit_low = int(request.form.get('take_profit_low'))
            take_profit_high = int(request.form.get('take_profit_high'))
          
            # Database contains tests, so check if the test_name is unique
            if data_import_check == False:
                conn = backtester.db_connect()
                cur = conn.cursor()
                # Make sure the Test_Name is unique.
                cur.execute ('SELECT Test_Name FROM Test_Variable_Range WHERE Test_Name = ?', (test_name,))
                res = cur.fetchone()
                # Test_Name already exits, so ask user to input unique name
                if res:
                    flash(f'{test_name} is already in use.  Please enter a unique test name to proceed with testing.', category='error')
                    return render_template("run_tests.html")

            # Load raw data only once before running tests
            instrument_period_dict, test_variable_range_id = backtester.create_db(test_name, fast_ma_low, fast_ma_high,  
                slow_ma_low, slow_ma_high, stop_loss_low, stop_loss_high, take_profit_low, take_profit_high)
            
            session['test_variable_range_id'] = test_variable_range_id
            
            # Created nested list of variable sets
            variable_list, no_of_tests = backtester.cartesian_product(fast_ma_low, fast_ma_high, 
                    slow_ma_low, slow_ma_high, stop_loss_low, stop_loss_high, take_profit_low, take_profit_high, 
                    instrument_period_dict, test_variable_range_id)

            start_tm = datetime.now().replace(microsecond=0)

            # Use all physical CPU cores to run the tests quickly
            pool = psutil.cpu_count(logical=False)
            pool = mp.Pool(pool)
            
            # Start multiprocessing.  Each worker will run a test with a list of variables
            pool.map_async(backtester.run_test, variable_list).get()
            session['saved_results_exist'] = True
            
            pool = int(psutil.cpu_count(logical = False))
            end_tm = datetime.now().replace(microsecond=0)
            time_elapsed = round((end_tm-start_tm).total_seconds())

            # We have data, so all links can appear on nav bar
            session['data_exists'] = True

            conn = backtester.db_connect()
            cur = conn.cursor()
            # Get number of rows inserted
            cur.execute (f'''SELECT COUNT(Position_Details_ID) 
                            FROM Position_Details AS p
                            JOIN Strategy_Results as S
                            ON p.Strategy_Results_ID = s.Strategy_Results_ID
                            WHERE s.Test_Variable_Range_ID = ?''', (session['test_variable_range_id'],))
            res = list(cur.fetchone()) 
            no_of_inserts = res[0] + no_of_tests + 2
            
            if data_import_check == 'Data Imported':
                cur.execute ('SELECT COUNT(*) FROM Market_Data')
                res = list(cur.fetchone()) 
                no_of_inserts += res[0]
            
            if conn:
                conn.close()

            # Flash message about test stats
            flash(f'{pool} cores completed {no_of_tests:,d} tests and inserted {no_of_inserts:,d} records into the database in {time_elapsed} seconds', category='success')

            return redirect(url_for("views.results"))
        
    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        backtester.log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)

    return render_template("run_tests.html")


@views.route('/results/', methods=['GET', 'POST'])
def results():
    # Show results of test in desc order.  Select chart to open in new browser window

    try:
        session['data_exists'] = True
        # Display individual results
        if request.method == 'GET':
            top_results = backtester.retrieve_top_strats(session['test_variable_range_id'])
            session['top_results'] = top_results

            #Preload group data in case of test rerunning tests and nav clicking around
            top_group_results = backtester.retrieve_top_group_strats(session['test_variable_range_id'])
            session['top_group_results'] = top_group_results   
            return render_template("results.html", top_results=top_results)
    
        elif request.method == 'POST':
            # Display group results
            if 'group_results' in request.form:
                top_group_results = backtester.retrieve_top_group_strats(session['test_variable_range_id'])
                session['top_group_results'] = top_group_results
                return render_template("group_results.html", top_group_results=top_group_results)

            # View Chart by creating and opening html chart on HD
            elif 'strat_res_id' in request.form:
                backtester.plot_chart(request.form.get('strat_res_id'))
                url = os.getcwd() + '\\web\\templates\\ohcl_chart.html'
                webbrowser.open(url)

    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        backtester.log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)
               
    return render_template("results.html", top_results= session['top_results'])


@views.route('/group_results', methods=['GET', 'POST'])
def group_results():
    # Display group performance of top 200 individual performers

    try:
        if request.method == 'POST':
            # Display list of individual details of selected group
            session['idx'] = request.form.get('idx')
            session['selected_group_details'] = True
            return redirect(url_for("views.group_details"))

    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        backtester.log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)

    return render_template("group_results.html", top_group_results=session['top_group_results'])


@views.route('/group_details', methods=['GET', 'POST'])
def group_details():

    try:
        # Get the MAs for the group, adjust idx to python's start at 0
        idx = int(session['idx']) - 1
        fast_ma = session['top_group_results'][idx]['fast_ma']
        slow_ma = session['top_group_results'][idx]['slow_ma']

        # Retrieve all results for the top group performers
        conn = backtester.db_connect()
        cur = conn.cursor()
        cur.execute(f''' SELECT *
                        FROM Strategy_Results
                        WHERE Test_Variable_Range_ID = ? AND
                            Fast_MA = ? AND Slow_MA = ?
                        ORDER BY Total_PNL DESC''', 
                        (session['test_variable_range_id'], fast_ma, slow_ma))
            
        # Map column names to field values in nested dictionary
        rec_list = list(cur.fetchall())       
        col = [desc[0].lower() for desc in cur.description]
        group_details = []

        # Change float from 0.05 to 5%
        rec_list = [list(t) for t in rec_list]
        for x in range(len(rec_list)):
            rec_list[x][4] = str( round(rec_list[x][4]*100, 1)) + '%'
            rec_list[x][5] = str( round(rec_list[x][5]*100, 1)) + '%'
            rec_list[x][6] = str( round(rec_list[x][6]*100, 1)) + '%'

        # Create a  strategy results dictionary
        for row in rec_list:
            dic = dict(zip(col, row))
            group_details.append(dic)
        
        if request.method == 'POST':
        # View Chart by creating and opening html chart on HD
            if 'strat_res_id' in request.form:
                backtester.plot_chart(request.form.get('strat_res_id'))
                url = os.getcwd() + '\\web\\templates\\ohcl_chart.html'
                webbrowser.open(url)
    
    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        backtester.log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)
    
    return render_template("group_details.html", group_details=group_details)


@views.route('/saved_results', methods=['GET', 'POST'])
def saved_results():
    # Retrieve list of saved results from previous tests

    try:
        conn = backtester.db_connect()
        cur = conn.cursor()
        cur.execute ('SELECT * FROM Test_Variable_Range')
        res = cur.fetchall()
        col = [desc[0].lower() for desc in cur.description]
        test_name = []
        for row in res:
            dic = dict(zip(col, row))
            test_name.append(dic)

        # Redirect to the saved results of a previous test
        if request.method == 'POST':
            if 'test_variable_range_id' in request.form:
                session['test_variable_range_id'] = request.form.get('test_variable_range_id')
                return redirect(url_for("views.results"))

    except BaseException:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_path, f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)
        backtester.log_exceptions(f_path, f_name, exc_type, exc_obj, exc_tb.tb_lineno)
    
    return render_template("saved_results.html", test_name=test_name)