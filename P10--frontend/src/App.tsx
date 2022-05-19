import React from 'react';
import './styles/app.scss';
import {BrowserRouter as Router, Route, Routes} from "react-router-dom";
import MainLayout from "./layouts/_layout";
import {SidebarProvider} from "./contexts/sidebarContext";
import {SnackbarKey, SnackbarProvider} from 'notistack';
import {MapDetailsProvider} from "./contexts/mapDetailsContext";
import {HistogramProvider} from './contexts/histogramContext';
import SnackbarCloseButton from "./components/SnackbarCloseButton";

function App() {
    return (
        <Router>
            <SnackbarProvider action={key => <SnackbarCloseButton key={key}/>}
                              anchorOrigin={{vertical: 'bottom', horizontal: 'right'}}>
                <MapDetailsProvider>
                    <HistogramProvider>
                        <SidebarProvider>
                            <Routes>
                                <Route path={'/'} element={<MainLayout/>}/>
                            </Routes>
                        </SidebarProvider>
                    </HistogramProvider>
                </MapDetailsProvider>
            </SnackbarProvider>
        </Router>
    );
}

export default App;
