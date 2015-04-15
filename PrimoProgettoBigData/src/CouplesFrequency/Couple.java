package CouplesFrequency;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * la coppia di prodotti e l'effettiva quantita' venduta
 *
 */
public class Couple {
	public String couple;
	public Double percentage;

	public Couple(String couple, Double percentage) {
		this.couple = couple;
		this.percentage = percentage;
	}
};