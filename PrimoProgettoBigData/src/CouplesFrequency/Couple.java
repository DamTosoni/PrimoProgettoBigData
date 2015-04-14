package CouplesFrequency;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * la coppia di prodotti e l'effettiva quantita' venduta
 *
 */
public class Couple {
	public String couple;
	public Integer percentage;

	public Couple(String couple, Integer percentage) {
		this.couple = couple;
		this.percentage = percentage;
	}
};