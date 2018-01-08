/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mainwork;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nastja
 */
public class Connection {
    private static Cassandra cassandra;

    public Connection() {
    }
    
     public static Cassandra getInstance(){
      if (cassandra == null){

          try {
              cassandra = new Cassandra();
          } catch (Exception ex) {
              Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
          }

      }
      return cassandra;
  }
    
}
