/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
enum LogMachineState {
        BOOTSTRAP {
            LogMachineState progress() {
                return NORMAL;
            }
            
            boolean isBootstrapping() {
                return true;
            }
            
            boolean starting() {
                return true;
            }
            
            boolean acceptRecords() {
                return true;
            }  
        },
        NORMAL {
            LogMachineState shutdown() {
                return SHUTDOWN;
            }
            
            LogMachineState checkException(Exception e) throws RuntimeException {
                if ( e instanceof InterruptedException ) {
                    throw new RuntimeException(e);
                } else {
                    return super.checkException(e);
                }
            }
            
            boolean acceptRecords() {
                return true;
            }           
        },
        SHUTDOWN {
            LogMachineState idle() {
                return FINISHED;
            }
        },
        FINISHED {
            LogMachineState reset() {
                return IDLE;
            }
        },
        ERROR {
            LogMachineState shutdown() {
                return ERROR;
            }
            
            boolean isErrorState() {
                return true;
            }
            
            LogMachineState idle() {
                return FINISHED;
            }
        },
        IDLE {
            LogMachineState bootstrap() {
                return BOOTSTRAP;
            }         
        
            boolean starting() {
                return true;
            }

            boolean acceptRecords() {
                return true;
            }             
            
        };
        
        LogMachineState progress() {
            throw new RuntimeException(this + "  -- bad state");
        }
        
        LogMachineState shutdown() {
            throw new RuntimeException(this + "  -- bad state");
        }
        
        LogMachineState idle() {
            throw new RuntimeException(this + "  -- bad state");
        }
        
        LogMachineState bootstrap() {
            throw new RuntimeException(this + "  -- bad state");
        }
        
        LogMachineState reset() {
            throw new RuntimeException(this + "  -- bad state");
        }        
        
        boolean isErrorState() {
            return false;
        }
        
        boolean isBootstrapping() {
            return false;
        }
        
        boolean acceptRecords() {
            return false;
        }
        
        boolean starting() {
            return false;
        }        
        
        LogMachineState checkException(Exception e) throws RuntimeException {
            if ( e instanceof InterruptedException ) {
                return this;
            } else if ( e instanceof IOException ) {
                return ERROR;
            }
            throw new RuntimeException(e);
        }
}
