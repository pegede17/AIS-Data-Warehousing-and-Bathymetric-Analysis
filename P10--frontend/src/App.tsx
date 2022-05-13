import React from 'react';
import './styles/app.scss';
import {BrowserRouter as Router, Route, Routes} from "react-router-dom";
import MainLayout from "./layouts/_layout";
import {SidebarProvider} from "./contexts/sidebarContext";
import {SnackbarProvider} from 'notistack';
import {MapDetailsProvider} from "./contexts/mapDetailsContext";

function App() {
    return (
        <Router>
            <SnackbarProvider>
                <MapDetailsProvider>
                    <SidebarProvider>
                        <Routes>
                            <Route path={'/'} element={<MainLayout/>}/>
                        </Routes>
                    </SidebarProvider>
                </MapDetailsProvider>
            </SnackbarProvider>
        </Router>
    );
}

export default App;
