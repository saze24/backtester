from web import create_app

app = create_app()

# Run Backtester
if __name__ == '__main__':

    # Use app.run(debug=True) for development
    app.run()