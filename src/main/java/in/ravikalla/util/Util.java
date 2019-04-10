package in.ravikalla.util;

import in.ravikalla.model.Payor;

public class Util {
    public static boolean isValid(Payor objPayor) {
		if ((null == objPayor) || (null == objPayor.getName()))
			return false;
		else
			return true;
	}
}
