package kafka.spark.integration;

import java.io.IOException;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.log4j.Logger;

import scala.Tuple2;

public class filterMessage implements PairFunction<String,String, String[]> {
	private static final long serialVersionUID = 42l;
	
//	@Override
	public Tuple2<String,String[]> call(String dataMessage) {
		try{
			if (dataMessage !=null){
			String[] SplitMessage = dataMessage.split(";");
			String DateTime = SplitMessage[0];
			String Header = SplitMessage[1].substring(0,6);
			String MacAddr = SplitMessage[1].substring(7,23);
			double Temp = scaleTemperature(SplitMessage[1].substring(23,27), 1, 0);
			double Pow = scaleCT(SplitMessage[1].substring(27,31),1,1);
			double Vib = scaleVibration(SplitMessage[1].substring(31,35),1,1);
			double Aco = scaleSound(SplitMessage[1].substring(35,39),1,0);
			double Aux = scaleAuxDC(SplitMessage[1].substring(39,43),1,0);
			
			String[] DataArray = {DateTime,
					Double.toString(Temp),
					Double.toString(Pow),
					Double.toString(Vib),
					Double.toString(Aco),
					Double.toString(Aux)
					};
			
			return new Tuple2<String, String[]>(MacAddr,DataArray);
			}
		}
		catch(IOException ex){
			Logger LOG = Logger.getLogger(this.getClass());
			LOG.error("IO error while filtering messages",ex);
			LOG.trace(null,ex);
			}
		return null;
		}
	


public static double TypeKTCCharacterisation(double dVolts){
	//initializing variables 
	double[] dMVTablePos = {
			0.000, // 0'
		    0.397, // 10'
		    0.798, // 20'
		    1.203, // 30'
		    1.612, // 40'
		    2.023, // 50'
		    2.436, // 60'
		    2.851, // 70'
		    3.267, // 80'
		    3.682, // 90'
		    4.096, // 100'
		    4.509, // 110'
		    4.920, // 120'
		    5.328, // 130'
		    5.735, // 140'
		    6.138, // 150'
		    6.540, // 160'
		    6.941, // 170'
		    7.340, // 180'
		    7.739, // 190'
		    8.138, // 200'
		    8.539, // 210'
		    8.940, // 220'
		    9.343, // 230'
		    9.747, // 240'
		    10.153, // 250'
		    10.561, // 260'
		    10.971, // 270'
		    11.382, // 280'
		    11.795, // 290'
		    12.209, // 300'
		    12.624, // 310'
		    13.040, // 320'
		    13.457, // 330'
		    13.874, // 340'
		    14.293, // 350'
		    14.713, // 360'
		    15.133, // 370'
		    15.554, // 380'
		    15.975, // 390'
		    16.397, // 400'
		    16.820, // 410'
		    17.243, // 420'
		    17.667, // 430'
		    18.091, // 440'
		    18.516, // 450'
		    18.941, // 460'
		    19.366, // 470'
		    19.792, // 480'
		    20.218, // 490'
		    20.644, // 500'
		    21.071, // 510'
		    21.497, // 520'
		    21.924, // 530'
		    22.350, // 540'
		    22.776, // 550'
		    23.203, // 560'
		    23.629, // 570'
		    24.055, // 580'
		    24.480, // 590'
		    24.905, // 600'
		    25.330, // 610'
		    25.755, // 620'
		    26.179, // 630'
		    26.602, // 640'
		    27.025, // 650'
		    27.447, // 660'
		    27.869, //670'
		    28.289, // 680'
		    28.710, // 690'
		    29.129, // 700'
		    29.548, // 710'
		    29.965, // 720'
		    30.382, // 730'
		    30.798, // 740'
		    31.213, // 750'
		    31.628, // 760'
		    32.041, // 770'
		    32.453, // 780'
		    32.865, // 790'
		    33.275, // 800'
		    33.685, // 810'
		    34.093, // 820'
		    34.501, // 830'
		    34.908, // 840'
		    35.313, // 850'
		    35.718, // 860'
		    36.121, // 870'
		    36.524, // 880'
		    36.925, // 890'
		    37.326, // 900'
		    37.725, // 910'
		    38.124, // 920'
		    38.522, // 930'
		    38.918, // 940'
		    39.314, // 950'
		    39.708, // 960'
		    40.101, // 970'
		    40.494, // 980'
		    40.885, // 990'
		    41.276, // 1000'
		    41.665, // 1010'
		    42.053, // 1020'
		    42.440, // 1030'
		    42.826, // 1040'
		    43.211, // 1050'
		    43.595, // 1060'
		    43.978, // 1070'
		    44.359, // 1080'
		    44.740, // 1090'
		    45.119, // 1100'
		    45.497, // 1110'
		    45.873, // 1120'
		    46.249, // 1130'
		    46.623, // 1140'
		    46.995, // 1150'
		    47.367, // 1160'
		    47.737, // 1170'
		    48.105, // 1180'
		    48.473, // 1190'
		    48.838, // 1200'
		    49.202, // 1210'
		    49.565, // 1220'
		    49.926, // 1230'
		    50.286, // 1240'
		    50.644, // 1250'
		    51.000, // 1260'
		    51.355, // 1270'
		    51.708, // 1280'
		    52.060, // 1290'
		    52.410, // 1300'
		    52.759, // 1310'
		    53.106, // 1320'
		    53.451, // 1330'
		    53.795, // 1340'
		    54.138 // 1350'
		    };
	double[] dMVTableNeg = {
			0.000,
			-0.392,
			-0.778,
			-1.156,
			-1.527,
			-1.889,
			};
	
	double dMilliVolts = dVolts * 1000;
	double dTemp;
	int iSeg;
	
	if (dMilliVolts>= 0.0){
		// temperature is greater than 0
		dTemp = 0.0;
		for (iSeg = 0; iSeg< dMVTablePos.length -1; iSeg++){
			if (dMilliVolts <dMVTablePos[iSeg + 1]){
				break;
				}
			dTemp = dTemp + 10.0;
			}
		if (iSeg == dMVTablePos.length){
			// over-range
			dTemp = 1300.00; // Upscale
			}else{
			dTemp = dTemp +10.0* (dMilliVolts - dMVTablePos[iSeg])/(dMVTablePos[iSeg + 
			                                                                    1]-dMVTablePos[iSeg]);
			}
		}else {
		// temperature is below 0
		dTemp = 0.0;
		for(iSeg = 0; iSeg<dMVTableNeg.length -1; iSeg++){
			if (dMilliVolts > dMVTableNeg[iSeg + 1]){
				break;
				}
			dTemp = dTemp - 10.0;
			}
		if (iSeg == dMVTableNeg.length)
		{
			// under- range
			dTemp = -200.0; // Downscale
			}else{
			dTemp = dTemp - 10.0 * (dMilliVolts - dMVTableNeg[iSeg]) / (dMVTableNeg[iSeg +
			                                                                          1] - dMVTableNeg[iSeg]);
			}
		}
	return(dTemp);
	}

public static double scaleTemperature(String iVal, int ExtTempSpan, int ExtTempZero){
	int bitmask = 0xFFFC;
	int iVal2 = Integer.parseInt(iVal,16) & bitmask;	// Convert string to hexadecimal value
	if (iVal2 > 0x7FFF){
		iVal2 = 65536 - iVal2;
	}
	double dEngValue = iVal2 *(0.25 /4);
	dEngValue = dEngValue * 0.000041276;
	dEngValue = TypeKTCCharacterisation(dEngValue); // applying NIST characterisation
	// finally, user calibration adjustment, (y = Mx + C)
	dEngValue = ExtTempSpan * dEngValue + ExtTempZero;
	return(dEngValue);
	}

public static double scaleCT (String iVal, int PowerCTSpan, int PowerCTZero){
	int iVal2 = Integer.parseInt(iVal,16);	// Convert string to hexadecimal value
	double dEngValue = iVal2 /30;			// Because iVal is 30 RMS of ADC value
	dEngValue = 3.3 * dEngValue / 1023;		// ADC pin volts as RMS
	dEngValue = dEngValue / 3.3;			// Remove input gain (33/10), now we are at terminal volts as RMS
	// NB. for Mane CT's 0.333 V is full range (e.g. 20A)
	// So user calibration for 20A range would be M = 20/0.33 = 60.05, C = 0
	
	// finally, appling the user calibration adjustment, (y = Mx +C)
	dEngValue = PowerCTSpan * dEngValue + PowerCTZero;
	return(dEngValue);
	}

public static double scaleSound (String iVal, int SoundSpan, int SoundZero){
	int iVal2 = Integer.parseInt(iVal,16);	// Convert string to hexadecimal value
	double dEngValue = iVal2 / 30;			// Because iVal is 30 RMS of ADC value
	dEngValue = 3.3 *dEngValue / 1023;		// ADC pin volts as RMS
	
	// for Knowles mic, sensitivity is -42dB nominal (i.e. S = 7.94mV/pa)
	// for RS mic, sensitivity is is -40dB nominal (i.e. S = 10.0mV/pa)
	// SPL = 151.8 - 20 * log10(S) + 20 * log10 (MicV / 0.7746)
	
	dEngValue = 151.8 - 20 * Math.log10(10.0) + 20 * Math.log10(dEngValue / 0.7746);
	
	//finally, applying the user calibratiokn adjustment, (y = Mx + C)
	
	dEngValue = SoundSpan * dEngValue + SoundZero;
	return dEngValue;
	}

public static double scaleVibration (String iVal, int VibSpan, int VibZero){
	int iVal2 = Integer.parseInt(iVal,16);	// Convert string to hexadecimal value
	double dEngValue = iVal2 * (1000.0 / 4096);			// iVal originally in RMS (49=096 = 1g) now in 'mg'
	dEngValue = 3.3 *dEngValue / 1023;		// ADC pin volts as RMS
	
	//applying user calibration adjustment, (y = Mx + C)
	
	dEngValue = VibSpan * dEngValue + VibZero;
	return (dEngValue);
	}

public static double scaleAuxDC (String iVal, int AuxSpan, int AuxZero){
	int iVal2 = Integer.parseInt(iVal,16);	// Convert string to hexadecimal value
	double dEngValue = 3.3 * iVal2 / 1023;			//  ADC value to ADC pin volts
	dEngValue = 10.56 * dEngValue / 3.3;	// External input (or PSU) vlots

	// applying user calibration adjustment, (y = Mx + C)
	
	dEngValue = AuxSpan * dEngValue + AuxZero;
	return (dEngValue);
	}

}
