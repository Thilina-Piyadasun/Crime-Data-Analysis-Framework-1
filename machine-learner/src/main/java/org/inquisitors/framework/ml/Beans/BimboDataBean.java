package org.inquisitors.framework.ml.Beans;

import java.io.Serializable;

/**
 * Created by Thilina on 8/17/2016.
 */
public class BimboDataBean  implements Serializable{

    private int Semana;
    private int Agencia_ID ;
    private int Canal_ID;
    private int Ruta_SAK;
    private int Cliente_ID;
    private int Producto_ID;

    private double Demnda;

    public BimboDataBean(String semana, String agencia_ID, String canal_ID, String ruta_SAK, String cliente_ID, String producto_ID,  String demnda) {

        Semana = Integer.parseInt(semana);
        Agencia_ID = Integer.parseInt(agencia_ID);
        Canal_ID = Integer.parseInt(canal_ID);
        Ruta_SAK = Integer.parseInt(ruta_SAK);
        Cliente_ID = Integer.parseInt(cliente_ID);
        Producto_ID = Integer.parseInt(producto_ID);

        Demnda = Double.parseDouble(demnda);
    }

    public BimboDataBean(String semana, String agencia_ID, String canal_ID, String ruta_SAK, String cliente_ID, String producto_ID) {

        Semana = Integer.parseInt(semana);
        Agencia_ID = Integer.parseInt(agencia_ID);
        Canal_ID = Integer.parseInt(canal_ID);
        Ruta_SAK = Integer.parseInt(ruta_SAK);
        Cliente_ID = Integer.parseInt(cliente_ID);
        Producto_ID = Integer.parseInt(producto_ID);

    }
    public double getSemana() {
        return Semana;
    }

    public int getAgencia_ID() {
        return Agencia_ID;
    }

    public int getCanal_ID() {
        return Canal_ID;
    }

    public int getRuta_SAK() {
        return Ruta_SAK;
    }

    public int getCliente_ID() {
        return Cliente_ID;
    }

    public int getProducto_ID() {
        return Producto_ID;
    }

    public double getDemnda() {
        return Demnda;
    }
}
