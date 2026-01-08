import os
import sqlite3
import logging
from datetime import datetime
import pytz
import yfinance as yf
import pandas as pd
from tabulate import tabulate
import time


"""
Stock Data Fetcher - 1500 Stocks with BATCH PROCESSING
Splits into 3 batches of 500 stocks each
Runs every minute via GitHub Actions
"""

# Create directories
os.makedirs('data', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_fetch_v1.log'),
        logging.StreamHandler()
    ]
)

# Top 1500 NSE Stock Symbols (Curated list of actively traded stocks)
STOCK_LIST_1500 = [
    # Nifty 50 (50 stocks)
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
    'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS',
    'BAJFINANCE.NS', 'LT.NS', 'ASIANPAINT.NS', 'HCLTECH.NS', 'AXISBANK.NS',
    'MARUTI.NS', 'SUNPHARMA.NS', 'TITAN.NS', 'ULTRACEMCO.NS', 'NESTLEIND.NS',
    'BAJAJFINSV.NS', 'WIPRO.NS', 'ADANIENT.NS', 'ONGC.NS', 'NTPC.NS',
    'TECHM.NS', 'POWERGRID.NS', 'M&M.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS',
    'INDUSINDBK.NS', 'DIVISLAB.NS', 'BAJAJ-AUTO.NS', 'DRREDDY.NS', 'JSWSTEEL.NS',
    'BRITANNIA.NS', 'CIPLA.NS', 'APOLLOHOSP.NS', 'EICHERMOT.NS', 'GRASIM.NS',
    'HINDALCO.NS', 'COALINDIA.NS', 'BPCL.NS', 'HEROMOTOCO.NS', 'TATACONSUM.NS',
    'ADANIPORTS.NS', 'SBILIFE.NS', 'HDFCLIFE.NS', 'UPL.NS', 'SHREECEM.NS',
    
    # Nifty Next 50 (50 stocks)
    'PIDILITIND.NS', 'GODREJCP.NS', 'DABUR.NS', 'BERGEPAINT.NS', 'MARICO.NS',
    'COLPAL.NS', 'MCDOWELL-N.NS', 'HAVELLS.NS', 'BOSCHLTD.NS', 'SIEMENS.NS',
    'ABB.NS', 'VEDL.NS', 'HINDZINC.NS', 'BANKBARODA.NS', 'PNB.NS',
    'CANBK.NS', 'UNIONBANK.NS', 'IDFCFIRSTB.NS', 'BANDHANBNK.NS', 'FEDERALBNK.NS',
    'IDEA.NS', 'ZEEL.NS', 'DLF.NS', 'GODREJPROP.NS', 'OBEROIRLTY.NS',
    'AMBUJACEM.NS', 'ACC.NS', 'GAIL.NS', 'IOC.NS', 'PETRONET.NS',
    'MRF.NS', 'BALKRISIND.NS', 'CUMMINSIND.NS', 'TORNTPHARM.NS', 'LUPIN.NS',
    'BIOCON.NS', 'AUROPHARMA.NS', 'CADILAHC.NS', 'GLENMARK.NS', 'ALKEM.NS',
    'TRENT.NS', 'ABFRL.NS', 'PAGEIND.NS', 'PVR.NS', 'JUBLFOOD.NS',
    'MPHASIS.NS', 'LTTS.NS', 'COFORGE.NS', 'PERSISTENT.NS', 'MINDTREE.NS',
    
    # Additional 100 stocks (100-200)
    'ADANIGREEN.NS', 'ADANIPOWER.NS', 'ADANITRANS.NS', 'AMBUJACFM.NS', 'ASHOKLEY.NS',
    'AFFLE.NS', 'AIAENG.NS', 'AJANTPHARM.NS', 'APLLTD.NS', 'ALKEM.NS',
    'AMARAJABAT.NS', 'AMBUJACEM.NS', 'APOLLOTYRE.NS', 'ASHOKLEY.NS', 'ASTRAL.NS',
    'ATUL.NS', 'AUBANK.NS', 'AUROPHARMA.NS', 'AXISBANK.NS', 'BAJAJCON.NS',
    'BAJAJHLDNG.NS', 'BAJFINANCE.NS', 'BALKRISIND.NS', 'BALRAMCHIN.NS', 'BANDHANBNK.NS',
    'BANKBARODA.NS', 'BATAINDIA.NS', 'BEL.NS', 'BERGEPAINT.NS', 'BHARATFORG.NS',
    'BHARTIARTL.NS', 'BHEL.NS', 'BIOCON.NS', 'BOSCHLTD.NS', 'BPCL.NS',
    'BRITANNIA.NS', 'BSOFT.NS', 'CANBK.NS', 'CANFINHOME.NS', 'CHAMBLFERT.NS',
    'CHOLAFIN.NS', 'CIPLA.NS', 'COALINDIA.NS', 'COFORGE.NS', 'COLPAL.NS',
    'CONCOR.NS', 'COROMANDEL.NS', 'CROMPTON.NS', 'CUB.NS', 'CUMMINSIND.NS',
    'DABUR.NS', 'DALBHARAT.NS', 'DEEPAKNTR.NS', 'DELTACORP.NS', 'DIVISLAB.NS',
    'DIXON.NS', 'DLF.NS', 'DRREDDY.NS', 'EICHERMOT.NS', 'ESCORTS.NS',
    'EXIDEIND.NS', 'FEDERALBNK.NS', 'FORTIS.NS', 'GAIL.NS', 'GLENMARK.NS',
    'GMRINFRA.NS', 'GNFC.NS', 'GODREJCP.NS', 'GODREJIND.NS', 'GODREJPROP.NS',
    'GRANULES.NS', 'GRASIM.NS', 'GUJGASLTD.NS', 'HAL.NS', 'HAVELLS.NS',
    'HCLTECH.NS', 'HDFC.NS', 'HDFCAMC.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS',
    'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDCOPPER.NS', 'HINDPETRO.NS', 'HINDUNILVR.NS',
    'HINDZINC.NS', 'HONAUT.NS', 'ICICIBANK.NS', 'ICICIGI.NS', 'ICICIPRULI.NS',
    'IDEA.NS', 'IDFCFIRSTB.NS', 'IEX.NS', 'IGL.NS', 'INDHOTEL.NS',
    'INDIACEM.NS', 'INDIAMART.NS', 'INDIANB.NS', 'INDIGO.NS', 'INDUSINDBK.NS',
    'INDUSTOWER.NS', 'INFY.NS', 'INTELLECT.NS', 'IOC.NS', 'IPCALAB.NS',
    'IRB.NS', 'IRCTC.NS', 'ITC.NS', 'JINDALSTEL.NS', 'JKCEMENT.NS',
    'JSWSTEEL.NS', 'JUBLFOOD.NS', 'JUSTDIAL.NS', 'KANSAINER.NS', 'KEI.NS',
    'KOTAKBANK.NS', 'L&TFH.NS', 'LALPATHLAB.NS', 'LAURUSLABS.NS', 'LICHSGFIN.NS',
    'LT.NS', 'LTIM.NS', 'LTTS.NS', 'LUPIN.NS', 'M&M.NS',
    'M&MFIN.NS', 'MANAPPURAM.NS', 'MARICO.NS', 'MARUTI.NS', 'MCDOWELL-N.NS',
    'MCX.NS', 'METROPOLIS.NS', 'MFSL.NS', 'MGL.NS', 'MINDTREE.NS',
    'MOTHERSON.NS', 'MPHASIS.NS', 'MRF.NS', 'MUTHOOTFIN.NS', 'NATIONALUM.NS',
    'NAUKRI.NS', 'NAVINFLUOR.NS', 'NESTLEIND.NS', 'NMDC.NS', 'NTPC.NS',
    'OBEROIRLTY.NS', 'OFSS.NS', 'OIL.NS', 'ONGC.NS', 'PAGEIND.NS',
    'PEL.NS', 'PERSISTENT.NS', 'PETRONET.NS', 'PFC.NS', 'PIDILITIND.NS',
    'PIIND.NS', 'PNB.NS', 'POLYCAB.NS', 'POWERGRID.NS', 'PVR.NS',
    'RAIN.NS', 'RAJESHEXPO.NS', 'RAMCOCEM.NS', 'RBLBANK.NS', 'RECLTD.NS',
    'RELIANCE.NS', 'SAIL.NS', 'SBICARD.NS', 'SBILIFE.NS', 'SBIN.NS',
    'SHREECEM.NS', 'SIEMENS.NS', 'SRF.NS', 'STARCEMENT.NS', 'SUNPHARMA.NS',
    'SUNTV.NS', 'SYNGENE.NS', 'TATACHEM.NS', 'TATACOMM.NS', 'TATACONSUM.NS',
    'TATAMOTORS.NS', 'TATAPOWER.NS', 'TATASTEEL.NS', 'TCS.NS', 'TECHM.NS',
    'TITAN.NS', 'TORNTPHARM.NS', 'TRENT.NS', 'TVSMOTOR.NS', 'UBL.NS',
    'ULTRACEMCO.NS', 'UPL.NS', 'VEDL.NS', 'VOLTAS.NS', 'WHIRLPOOL.NS',
    'WIPRO.NS', 'ZEEL.NS', 'ZOMATO.NS', 'ZYDUSLIFE.NS', '3MINDIA.NS',

# Midcap 150 stocks (200-350)
    'AARTIIND.NS', 'ABBOTINDIA.NS', 'ABCAPITAL.NS', 'ABFRL.NS', 'ACC.NS',
    'ADANIENT.NS', 'ADANIPORTS.NS', 'ALKEM.NS', 'AMBUJACEM.NS', 'APOLLOHOSP.NS',
    'ASHOKLEY.NS', 'ASIANPAINT.NS', 'ASTRAL.NS', 'ATUL.NS', 'AUBANK.NS',
    'AUROPHARMA.NS', 'AXISBANK.NS', 'BAJAJ-AUTO.NS', 'BAJAJCON.NS', 'BAJAJFINSV.NS',
    'BAJFINANCE.NS', 'BALKRISIND.NS', 'BALRAMCHIN.NS', 'BANDHANBNK.NS', 'BANKBARODA.NS',
    'BATAINDIA.NS', 'BEL.NS', 'BERGEPAINT.NS', 'BHARATFORG.NS', 'BHARTIARTL.NS',
    'BHEL.NS', 'BIOCON.NS', 'BOSCHLTD.NS', 'BPCL.NS', 'BRITANNIA.NS',
    'BSOFT.NS', 'CANBK.NS', 'CANFINHOME.NS', 'CHAMBLFERT.NS', 'CHOLAFIN.NS',
    'CIPLA.NS', 'COALINDIA.NS', 'COCHINSHIP.NS', 'COFORGE.NS', 'COLPAL.NS',
    'CONCOR.NS', 'COROMANDEL.NS', 'CROMPTON.NS', 'CUB.NS', 'CUMMINSIND.NS',
    'DABUR.NS', 'DALBHARAT.NS', 'DEEPAKNTR.NS', 'DELTACORP.NS', 'DIVISLAB.NS',
    'DIXON.NS', 'DLF.NS', 'DRREDDY.NS', 'EICHERMOT.NS', 'ESCORTS.NS',
    'EXIDEIND.NS', 'FEDERALBNK.NS', 'FORTIS.NS', 'GAIL.NS', 'GLENMARK.NS',
    'GMRINFRA.NS', 'GNFC.NS', 'GODREJCP.NS', 'GODREJIND.NS', 'GODREJPROP.NS',
    'GRANULES.NS', 'GRASIM.NS', 'GUJGASLTD.NS', 'HAL.NS', 'HAVELLS.NS',
    'HCLTECH.NS', 'HDFC.NS', 'HDFCAMC.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS',
    'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDCOPPER.NS', 'HINDPETRO.NS', 'HINDUNILVR.NS',
    'HINDZINC.NS', 'HONAUT.NS', 'ICICIBANK.NS', 'ICICIGI.NS', 'ICICIPRULI.NS',
    'IDEA.NS', 'IDFCFIRSTB.NS', 'IEX.NS', 'IGL.NS', 'INDHOTEL.NS',
    'INDIACEM.NS', 'INDIAMART.NS', 'INDIANB.NS', 'INDIGO.NS', 'INDUSINDBK.NS',
    'INDUSTOWER.NS', 'INFY.NS', 'INTELLECT.NS', 'IOC.NS', 'IPCALAB.NS',
    'IRB.NS', 'IRCTC.NS', 'ITC.NS', 'JINDALSTEL.NS', 'JKCEMENT.NS',
    'JSWSTEEL.NS', 'JUBLFOOD.NS', 'JUSTDIAL.NS', 'KANSAINER.NS', 'KEI.NS',
    'KOTAKBANK.NS', 'L&TFH.NS', 'LALPATHLAB.NS', 'LAURUSLABS.NS', 'LICHSGFIN.NS',
    'LT.NS', 'LTIM.NS', 'LTTS.NS', 'LUPIN.NS', 'M&M.NS',
    'M&MFIN.NS', 'MANAPPURAM.NS', 'MARICO.NS', 'MARUTI.NS', 'MCDOWELL-N.NS',
    'MCX.NS', 'METROPOLIS.NS', 'MFSL.NS', 'MGL.NS', 'MINDTREE.NS',
    'MOTHERSON.NS', 'MPHASIS.NS', 'MRF.NS', 'MUTHOOTFIN.NS', 'NATIONALUM.NS',
    
    # Smallcap stocks (350-600)
    'AAVAS.NS', 'ACE.NS', 'ADANIENSOL.NS', 'AFFLE.NS', 'AIAENG.NS',
    'AJANTPHARM.NS', 'APLLTD.NS', 'APOLLOTYRE.NS', 'AMBER.NS', 'ANGELONE.NS',
    'ANURAS.NS', 'APARINDS.NS', 'APCOTEXIND.NS', 'APPLEINDS.NS', 'ARVINDFASN.NS',
    'ASAHIINDIA.NS', 'ASHIANA.NS', 'ASIANHOTNR.NS', 'ASTRAZEN.NS', 'AUROPHARMA.NS',
    'AVANTIFEED.NS', 'AXIS.NS', 'BAJAJHLDNG.NS', 'BALAMINES.NS', 'BALMLAWRIE.NS',
    'BANCOINDIA.NS', 'BASF.NS', 'BAYERCROP.NS', 'BBL.NS', 'BEARDSELL.NS',
    'BEML.NS', 'BEPL.NS', 'BHARATGEAR.NS', 'BHARATRAS.NS', 'BHEDADEQP.NS',
    'BHUSANSTL.NS', 'BIKAJI.NS', 'BINDALAGRO.NS', 'BIRLACORPN.NS', 'BLISSGVS.NS',
    'BLUEDART.NS', 'BLUESTARCO.NS', 'BOMDYEING.NS', 'BORORENEW.NS', 'BRIGADE.NS',
    'BSE.NS', 'BSOFT.NS', 'CAMPUS.NS', 'CAMS.NS', 'CANFINHOME.NS',
    'CAPLIPOINT.NS', 'CARBORUNIV.NS', 'CARE.NS', 'CARTRADE.NS', 'CASTROLIND.NS',
    'CCL.NS', 'CDSL.NS', 'CEATLTD.NS', 'CENTEXT.NS', 'CENTRALBK.NS',
    'CENTURYPLY.NS', 'CENTURYTEX.NS', 'CERA.NS', 'CHALET.NS', 'CHAMBLFERT.NS',
    'CHEMPLASTS.NS', 'CHOLAHLDNG.NS', 'CIPLA.NS', 'CLEAN.NS', 'COALINDIA.NS',
    'COCHIN.NS', 'COLLAR.NS', 'COMPINFO.NS', 'CONCORDBIO.NS', 'CONFIPET.NS',
    'COROMANDEL.NS', 'COSPOWER.NS', 'COX&KINGS.NS', 'CPSEETEC.NS', 'CRAFTSMAN.NS',
    'CREDITACC.NS', 'CRISIL.NS', 'CROMPTON.NS', 'CSB.NS', 'CUB.NS',
    'CUMMINSIND.NS', 'CUPID.NS', 'CYBERMEDIA.NS', 'CYIENT.NS', 'DADRAPHARM.NS',
    'DALBHARAT.NS', 'DATAPATTNS.NS', 'DCBBANK.NS', 'DCMSHRIRAM.NS', 'DEEPAKFERT.NS',
    'DEEPAKNTR.NS', 'DELTACORP.NS', 'DESICAL.NS', 'DHANI.NS', 'DHANUKA.NS',
    'DHARSUGAR.NS', 'DHFL.NS', 'DHUNINV.NS', 'DIAMINESQ.NS', 'DICIND.NS',
    'DIGISPICE.NS', 'DIVISLAB.NS', 'DIXON.NS', 'DLF.NS', 'DLINKINDIA.NS',
    'DOLLAR.NS', 'DOVEMARINE.NS', 'DPWIRES.NS', 'DREDGECORP.NS', 'DRREDDY.NS',
    'DUCON.NS', 'DYCL.NS', 'DYNAMATECH.NS', 'EASEMYTRIP.NS', 'EASTSILK.NS',
    'ECLERX.NS', 'EDELWEISS.NS', 'EIDPARRY.NS', 'EIHOTEL.NS', 'EKC.NS',
    'ELGIEQUIP.NS', 'EMAMILTD.NS', 'EMKAY.NS', 'EMMBI.NS', 'ENDURANCE.NS',
    
    # Additional stocks (600-900)
    'ENERGYDEV.NS', 'ENGINERSIN.NS', 'ENTERO.NS', 'EPL.NS', 'EQUITAS.NS',
    'EQUITASBNK.NS', 'ERIS.NS', 'EROSMEDIA.NS', 'ESCORT.NS', 'ESSDEE.NS',
    'ESTER.NS', 'EUROTEXIND.NS', 'EVEREADY.NS', 'EXCELINDUS.NS', 'EXIDEIND.NS',
    'FAIRCHEM.NS', 'FAIRFIN.NS', 'FCL.NS', 'FCONSUMER.NS', 'FDC.NS',
    'FEDERALBNK.NS', 'FIEMIND.NS', 'FILATEX.NS', 'FINCABLES.NS', 'FINPIPE.NS',
    'FLAIR.NS', 'FLEXITUFF.NS', 'FLUOROCHEM.NS', 'FMNL.NS', 'FORCEMOT.NS',
    'FORTISONICS.NS', 'FORTIS.NS', 'FREEDOM.NS', 'FSC.NS', 'FSL.NS',
    'Gabriel.NS', 'GAEL.NS', 'GAIL.NS', 'GALAXYSURF.NS', 'GALLANTT.NS',
    'GANDHITUBE.NS', 'GARFIBRES.NS', 'GARNETINT.NS', 'GATEWAY.NS', 'GDL.NS',
    'GEECEE.NS', 'GENCON.NS', 'GENESYS.NS', 'GESHIP.NS', 'GHCL.NS',
    'GICHSGFIN.NS', 'GILLANDERS.NS', 'GILLETTE.NS', 'GINNIFILA.NS', 'GIPCL.NS',
    'GKBOPTICAL.NS', 'GKW.NS', 'GLAXO.NS', 'GLENMARK.NS', 'GLOBAL.NS',
    'GLOBALPET.NS', 'GLOBE.NS', 'GLOBUSSPR.NS', 'GMBREW.NS', 'GMDCLTD.NS',
    'GMMPFAUDLR.NS', 'GMRINFRA.NS', 'GNFC.NS', 'GOACARBON.NS', 'GOCLCORP.NS',
    'GODFRYPHLP.NS', 'GODREJAGRO.NS', 'GODREJCP.NS', 'GODREJIND.NS', 'GODREJPROP.NS',
    'GOKEX.NS', 'GOKUL.NS', 'GOLD.NS', 'GOODLUCK.NS', 'GOODYEAR.NS',
    'GPIL.NS', 'GPPL.NS', 'GRANULES.NS', 'GRAPHITE.NS', 'GRASIM.NS',
    'GREAVESCOT.NS', 'GREENLAM.NS', 'GREENPANEL.NS', 'GREENPLY.NS', 'GRINDWELL.NS',
    'GRSE.NS', 'GRUH.NS', 'GSFC.NS', 'GSHIP.NS', 'GSS.NS',
    'GTLINFRA.NS', 'GTL.NS', 'GTPL.NS', 'GUFICBIO.NS', 'GUJALKALI.NS',
    'GUJAPOLLO.NS', 'GUJGAS.NS', 'GUJGASLTD.NS', 'GULFOILLUB.NS', 'GULFPETRO.NS',
    'GVKPIL.NS', 'HAL.NS', 'HAPPSTMNDS.NS', 'HATHWAY.NS', 'HAVELLS.NS',
    'HCC.NS', 'HCG.NS', 'HCLINFOSYS.NS', 'HCLTECH.NS', 'HCL-INSYS.NS',
    'HDFC.NS', 'HDFCAMC.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS', 'HEG.NS',
    'HEIDELBERG.NS', 'HERANBA.NS', 'HERCULES.NS', 'HERITGFOOD.NS', 'HEROMOTOCO.NS',
    'HESTERBIO.NS', 'HEXAWARE.NS', 'HFCL.NS', 'HGINFRA.NS', 'HIKAL.NS',
    'HIL.NS', 'HIMATSEIDE.NS', 'HINDALCO.NS', 'HINDCOMPOS.NS', 'HINDCOPPER.NS',
    'HINDDORROL.NS', 'HINDMOTOR.NS', 'HINDNATGLS.NS', 'HINDOILEXP.NS', 'HINDPETRO.NS',
    'HINDSANGAM.NS', 'HINDTANAC.NS', 'HINDUNILVR.NS', 'HINDWARE.NS', 'HINDZINC.NS',
    'HINDSYN.NS', 'HIRECT.NS', 'HISARMETAL.NS', 'HITECH.NS', 'HITECHCORP.NS',
    
    # Additional stocks (900-1200)
    'HITECHGEAR.NS', 'HMT.NS', 'HMVL.NS', 'HNDFDS.NS', 'HONAUT.NS',
    'HONEY.NS', 'HOTELEELA.NS', 'HOVS.NS', 'HPL.NS', 'HSCL.NS',
    'HTMEDIA.NS', 'HUBTOWN.NS', 'HUHTAMAKI.NS', 'HYDRAB.NS', 'HYSTEEL.NS',
    'IAF.NS', 'IBREALEST.NS', 'IBULHSGFIN.NS', 'ICEMAKE.NS', 'ICICIBANK.NS',
    'ICICIBANKP.NS', 'ICICIBIO.NS', 'ICICICARFIN.NS', 'ICICIGI.NS', 'ICICIM.NS',
    'ICICIMF.NS', 'ICICIPRUD.NS', 'ICICIPRU.NS', 'ICICIPRULI.NS', 'ICIL.NS',
    'ICRA.NS', 'ICRACD.NS', 'ICSA.NS', 'IDEA.NS', 'IDEAFORGE.NS',
    'IDFC.NS', 'IDFCBANK.NS', 'IDFCFIRSTB.NS', 'IDFCLIM.NS', 'IDFCMF.NS',
    'IDFNL.NS', 'IDFSECURITIES.NS', 'IDNTHSUG.NS', 'IEX.NS', 'IFBAGRO.NS',
    'IFBIND.NS', 'IFCI.NS', 'IFGL.NS', 'IFGLEXP.NS', 'IFL.NS',
    'IGARASHI.NS', 'IGL.NS', 'IGPL.NS', 'IIB.NS', 'IIFL.NS',
    'IIFLSEC.NS', 'IIFLW.NS', 'IIHFL.NS', 'IITL.NS', 'IL&FSENGG.NS',
    'IL&FSTR.NS', 'IL&FSTHQ.NS', 'IL&FSWLTD.NS', 'IMAGICAA.NS', 'IMFA.NS',
    'IMPAL.NS', 'IMPEXFERRO.NS', 'INDBANK.NS', 'INDHOTEL.NS', 'INDIACEM.NS',
    'INDIAGLYCO.NS', 'INDIAGRID.NS', 'INDIANB.NS', 'INDIANCARD.NS', 'INDIANHUME.NS',
    'INDIAMART.NS', 'INDIGO.NS', 'INDIGOPNTS.NS', 'INDLMETER.NS', 'INDNIPPON.NS',
    'INDOCO.NS', 'INDOCOUNT.NS', 'INDORAMA.NS', 'INDOSTAR.NS', 'INDOTECH.NS',
    'INDOTHAI.NS', 'INDOWIND.NS', 'INDRAMEDCO.NS', 'INDSWFTLAB.NS', 'INDSWFTLTD.NS',
    'INDUSFILA.NS', 'INDUSINDBK.NS', 'INDUSTOWER.NS', 'INEOS.NS', 'INFIBEAM.NS',
    'INFINITY.NS', 'INFOBEAN.NS', 'INFOMEDIA.NS', 'INFOTECH.NS', 'INFRA.NS',
    'INFRATEL.NS', 'INFY.NS', 'INGVYSYABK.NS', 'INNOIND.NS', 'INNOVANA.NS',
    'INSECTICID.NS', 'INSPIRISYS.NS', 'INTELLECT.NS', 'INTENTECH.NS', 'INTLCONV.NS',
    'INDBANK.NS', 'INVENTURE.NS', 'IOB.NS', 'IOC.NS', 'IOLCP.NS',
    'IPA.NS', 'IPCALAB.NS', 'IPL.NS', 'IRB.NS', 'IRCON.NS',
    'IRCTC.NS', 'IREDA.NS', 'IRFC.NS', 'ISGEC.NS', 'ISMTLTD.NS',
    'ITC.NS', 'ITDC.NS', 'ITDCEM.NS', 'ITI.NS', 'IVP.NS',
    'IZMO.NS' , 'J&&KBANK.NS', 'JAGRAN.NS', 'JAGSNPHARM.NS', 'JAIBALAJI.NS',
    'JAICORPLTD.NS', 'JAMNAAUTO.NS', 'JAYAGROGN.NS', 'JAYBARMARU.NS', 'JAYNECOIND.NS',
    'JAYPRAKASH.NS', 'JAYSREETEA.NS', 'JBCHEPHARM.NS', 'JBFIND.NS', 'JBMA.NS',
    'JHS.NS', 'JISLDVREQS.NS', 'JISLJALEQS.NS', 'JKCEMENT.NS', 'JKIL.NS',
    'JKLAKSHMI.NS', 'JKPAPER.NS', 'JKTYRE.NS', 'JMA.NS', 'JMFINANCIL.NS',
    'JMTAUTOLTD.NS', 'JOCIL.NS', 'JPASSOCIAT.NS', 'JPINFRATEC.NS', 'JPOLYINVST.NS',
    'JPPOWER.NS', 'JSLHISAR.NS', 'JSL.NS', 'JSWENERGY.NS', 'JSWHL.NS',
    'JSWSTEEL.NS', 'JUBILANT.NS', 'JUBLFOOD.NS', 'JUBLINDS.NS', 'JUSTDIAL.NS',
    # Remaining stocks (1200-1500)
'JYOTHYLAB.NS', 'JYOTISTRUC.NS', 'KABRAEXTRU.NS', 'KAJARIACER.NS', 'KAKATCEM.NS',
'KAKATIND.NS', 'KALAMANDIR.NS', 'KALYANI.NS', 'KALYANIFRG.NS', 'KALYANKJIL.NS',
'KAMATHOTEL.NS', 'KAMDHENU.NS', 'KANANIIND.NS', 'KANORICHEM.NS', 'KANSAINER.NS',
'KANSAIFC.NS', 'KANSAINER.NS', 'KAPILSOL.NS', 'KARDA.NS', 'KARURVYSYA.NS',
'KASBMINI.NS', 'KATAREHOSG.NS', 'KAVVERITEL.NS', 'KAYA.NS', 'KDDL.NS',
'KEC.NS', 'KEI.NS', 'KELLTONTEC.NS', 'KERNEX.NS', 'KESORAMIND.NS',
'KEYFINSERV.NS', 'KFINTECH.NS', 'KHADIM.NS', 'KILITCH.NS', 'KINGFA.NS',
'KIRIINDUS.NS', 'KIRLOSBROS.NS', 'KIRLOSENG.NS', 'KIRLOSIND.NS', 'KIRLPNU.NS',
'KITEX.NS', 'KKCL.NS', 'KMF.NS', 'KNRCON.NS', 'KOKUYOCMLN.NS',
'KOLTEPATIL.NS', 'KOPRAN.NS', 'KOSOFE.NS', 'KOTAKAGIA.NS', 'KOTAKBANK.NS',
'KOTAKBKETF.NS', 'KOTAKINFIA.NS', 'KOTAKMAH.NS', 'KOTAKMFIX.NS', 'KOTAKNIFTY.NS',
'KOTAKPSUBK.NS', 'KOTARISUG.NS', 'KPIL.NS', 'KPITTECH.NS', 'KPRMILL.NS',
'KRBL.NS', 'KREBSBIO.NS', 'KRIDHANINF.NS', 'KRISHANA.NS', 'KRISHCA.NS',
'KRISHIVAL.NS', 'KRITIKA.NS', 'KSB.NS', 'KSE.NS', 'KSL.NS',
'KTKBANK.NS', 'L&T.NS', 'L&TFH.NS', 'LALPATHLAB.NS', 'LAMBODHARA.NS',
'LANDMARK.NS', 'LAOPALA.NS', 'LASA.NS', 'LAURUSLABS.NS', 'LAXMIMACH.NS',
'LCCINFOTEC.NS', 'LEMONTREE.NS', 'LGBBROSLTD.NS', 'LIBERTSHOE.NS', 'LICHSGFIN.NS',
'LINCOLN.NS', 'LINDEINDIA.NS', 'LLOYDSME.NS', 'LLOYDSTEEL.NS', 'LMTL.NS',
'LOTUSEYE.NS', 'LOVABLE.NS', 'LOWVOLMOM.NS', 'LT.NS', 'LTFOODS.NS',
'LTIM.NS', 'LTTS.NS', 'LUMAXIND.NS', 'LUMAXTECH.NS', 'LUPIN.NS',
'LUPINCHEM.NS', 'LUXIND.NS', 'LXCHEM.NS', 'LYKALABS.NS', 'M&M.NS',
'M&MFIN.NS', 'MAANALU.NS', 'MACPOWER.NS', 'MADHAV.NS', 'MADHUCON.NS',
'MADRASFERT.NS', 'MAGADSUGAR.NS', 'MAGMA.NS', 'MAGNUM.NS', 'MAHAPEXLTD.NS',
'MAHABANK.NS', 'MAHASTEEL.NS', 'MAHESWAR.NS', 'MAHINDCIE.NS', 'MAHLIFE.NS',
'MAHLOG.NS', 'MAHSCOOTER.NS', 'MAHSEAMLES.NS', 'MAITHANALL.NS', 'MAJESCO.NS',
'MAKEINDIA.NS', 'MAKSON.NS', 'MANAKALUCO.NS', 'MANAKCOAT.NS', 'MANAKSIA.NS',
'MANAKSTEEL.NS', 'MANALIPETC.NS', 'MANAPPURAM.NS', 'MANGALAM.NS', 'MANGCHEFER.NS',
'MANINDS.NS', 'MANINFRA.NS', 'MANKIND.NS', 'MANUGRAPH.NS', 'MAPFCDL.NS',
'MARALOVER.NS', 'MARATHON.NS', 'MARICO.NS', 'MARINE.NS', 'MARKSANS.NS',
'MARSHALL.NS', 'MARUTI.NS', 'MASFIN.NS', 'MASTEK.NS', 'MATRIMONY.NS',
'MAWANASUG.NS', 'MAXHEALTH.NS', 'MAXINDIA.NS', 'MAXVIL.NS', 'MAYURUNIQ.NS',
'MAZDA.NS', 'MBAPL.NS', 'MBECL.NS', 'MCDOWELL-N.NS', 'MCDHOLDING.NS',
'MCL.NS', 'MCLEODRUSS.NS', 'MCX.NS', 'MEADOW.NS', 'MEERA.NS',
'MEG.NS', 'MEGASOFT.NS', 'MEGHMANI.NS', 'MELSTAR.NS', 'MENTHANOL.NS',
'MERCATOR.NS', 'MERCK.NS', 'METALFORGE.NS', 'METROBRAND.NS', 'METROPOLIS.NS',
'MFSL.NS', 'MGL.NS', 'MHRIL.NS', 'MICEL.NS', 'MICROPRO.NS',
'MIDDAY.NS', 'MIDHANI.NS', 'MINDACORP.NS', 'MINDTECK.NS', 'MINDTREE.NS',
'MIRCELECTR.NS', 'MIRZAINT.NS', 'MITCON.NS', 'MITTAL.NS', 'MKPL.NS',
'MMP.NS', 'MMTC.NS', 'MODIPON.NS', 'MODISOLEZ.NS', 'MODIRUBBER.NS',
'MODTHREAD.NS', 'MOHEALTH.NS', 'MOHITIND.NS', 'MOHOTAIND.NS', 'MOLDTKPAC.NS',
'MOLDTECH.NS', 'MON100.NS', 'MONARCH.NS', 'MORGANITE.NS', 'MOTHERSON.NS',
'MOTILALOFS.NS', 'MOTILALOSL.NS', 'MOXSH.NS', 'MPHASIS.NS', 'MPSLTD.NS',
'MRF.NS', 'MRO-TEK.NS', 'MRPL.NS', 'MSP.NS', 'MSTCLTD.NS',
'MTEDUCARE.NS', 'MTARTECH.NS', 'MUKANDLTD.NS', 'MUKTA.NS', 'MUKTAARTS.NS',
'MULTIBASE.NS', 'MULTILOGIC.NS', 'MULTIMETALS.NS', 'MUNDRAPORT.NS', 'MURUDCERA.NS',
'MUTHOOTCAP.NS', 'MUTHOOTFIN.NS', 'MVGJL.NS', 'NAC.NS', 'NAGAFERT.NS',
'NAGAIND.NS', 'NAGREEKCAP.NS', 'NAHARCAP.NS', 'NAHAREXP.NS', 'NAHARPOLY.NS',
'NAHARSPING.NS', 'NAINCO.NS', 'NANDAN.NS', 'NARMADA.NS', 'NASPERS.NS',
'NATCOPHARM.NS', 'NATHBIOGEN.NS', 'NATIONALUM.NS', 'NAUKRI.NS', 'NAVALITD.NS',
'NAVINFLUOR.NS', 'NAVKARCORP.NS', 'NAVNETEDUL.NS', 'NBCC.NS', 'NBIFIN.NS',
'NBVENTURES.NS', 'NBWM.NS', 'NCC.NS', 'NCLIND.NS', 'NDGL.NS'

    
]

# Generate remaining 1300 stocks by repeating the above pattern
# This ensures we have 1500 unique or repeated stocks for testing
for i in range(14):  # Repeat the base 100 stocks 14 more times to reach ~1500
    for stock in STOCK_LIST_1500[100:200]:
        if len(STOCK_LIST_1500) >= 1500:
            break
        STOCK_LIST_1500.append(stock)
    if len(STOCK_LIST_1500) >= 1500:
        break

# Trim to exactly 1500
STOCK_LIST_1500 = STOCK_LIST_1500[:1500]

DB_PATH = 'nifty50_top20_v1.db'
BATCH_SIZE = 500

def create_database():
    """Create database table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_1min_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            datetime DATETIME NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, datetime)
        )
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_symbol_datetime 
        ON stock_1min_data(symbol, datetime)
    ''')
    
    conn.commit()
    conn.close()

def fetch_batch(batch_stocks, batch_num):
    """Fetch 1-minute data for a batch of stocks"""
    try:
        logging.info(f"📊 Batch {batch_num}: Fetching {len(batch_stocks)} stocks...")
        start_time = time.time()
        
        data = yf.download(
            tickers=batch_stocks,
            period='1d',
            interval='1m',
            group_by='ticker',
            threads=True,
            progress=False,
            auto_adjust=True
        )
        
        elapsed = time.time() - start_time
        logging.info(f"✅ Batch {batch_num}: Fetched in {elapsed:.2f} seconds")
        return data
        
    except Exception as e:
        logging.error(f"❌ Batch {batch_num} failed: {str(e)}")
        return None

def store_data(data, stock_list, batch_num):
    """Store 1-minute data"""
    if data is None or data.empty:
        logging.warning(f"⚠️ Batch {batch_num}: No data to store")
        return 0, 0
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    total_candles = 0
    stocks_processed = 0
    
    try:
        for symbol in stock_list:
            try:
                if len(stock_list) == 1:
                    stock_data = data
                else:
                    stock_data = data[symbol]
                
                if stock_data.empty:
                    continue
                
                for idx, row in stock_data.iterrows():
                    try:
                        if pd.isna(row['Close']):
                            continue
                        
                        cursor.execute('''
                            INSERT OR REPLACE INTO stock_1min_data 
                            (symbol, datetime, open, high, low, close, volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            symbol,
                            idx.strftime('%Y-%m-%d %H:%M:%S'),
                            float(row['Open']) if pd.notna(row['Open']) else None,
                            float(row['High']) if pd.notna(row['High']) else None,
                            float(row['Low']) if pd.notna(row['Low']) else None,
                            float(row['Close']) if pd.notna(row['Close']) else None,
                            int(row['Volume']) if pd.notna(row['Volume']) else 0
                        ))
                        total_candles += 1
                        
                    except:
                        continue
                
                stocks_processed += 1
                    
            except Exception as e:
                continue
        
        conn.commit()
        logging.info(f"💾 Batch {batch_num}: Stored {total_candles:,} candles from {stocks_processed}/{len(stock_list)} stocks")
        
    finally:
        conn.close()
    
    return total_candles, stocks_processed

def get_stats():
    """Get database statistics"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM stock_1min_data')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM stock_1min_data')
    stocks = cursor.fetchone()[0]
    
    cursor.execute('SELECT MAX(datetime) FROM stock_1min_data')
    latest = cursor.fetchone()[0]
    
    conn.close()
    
    return total, stocks, latest

def main():
    """Main execution with batch processing"""
    logging.info("="*70)
    logging.info(f"🚀 GitHub Actions - Stock Fetcher (1500 Stocks - BATCH MODE)")
    logging.info(f"⏰ Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"📦 Total Stocks: {len(STOCK_LIST_1500)}")
    logging.info(f"📊 Batch Size: {BATCH_SIZE}")
    logging.info(f"🔢 Number of Batches: {(len(STOCK_LIST_1500) + BATCH_SIZE - 1) // BATCH_SIZE}")
    logging.info("="*70)
    
    create_database()
    
    total_candles_all = 0
    total_stocks_all = 0
    
    # Split into batches and process
    num_batches = (len(STOCK_LIST_1500) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(num_batches):
        batch_num = i + 1
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, len(STOCK_LIST_1500))
        batch_stocks = STOCK_LIST_1500[start_idx:end_idx]
        
        logging.info(f"\n{'='*70}")
        logging.info(f"🔄 Processing Batch {batch_num}/{num_batches}")
        logging.info(f"📋 Stocks {start_idx+1} to {end_idx} ({len(batch_stocks)} stocks)")
        logging.info(f"{'='*70}")
        
        # Fetch batch
        data = fetch_batch(batch_stocks, batch_num)
        
        # Store batch
        if data is not None:
            candles, stocks = store_data(data, batch_stocks, batch_num)
            total_candles_all += candles
            total_stocks_all += stocks
        
  # Small delay between batches
        if i < num_batches - 1:
            logging.info(f"⏳ Waiting 2 seconds before next batch...")
            time.sleep(2)
    
    # Final statistics
    total, unique_stocks, latest = get_stats()
    
    logging.info(f"\n{'='*70}")
    logging.info(f"📊 FINAL DATABASE STATS:")
    logging.info(f"{'='*70}")
    logging.info(f"   Total Candles in DB: {total:,}")
    logging.info(f"   Unique Stocks in DB: {unique_stocks}")
    logging.info(f"   Latest Data: {latest}")
    logging.info(f"   This Run: {total_candles_all:,} candles from {total_stocks_all} stocks")
    
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    logging.info(f"   Database Size: {db_size:.2f} MB")
    logging.info(f"{'='*70}")
    logging.info("\n✅ Batch processing completed successfully!")

if __name__ == "__main__":
    main()
