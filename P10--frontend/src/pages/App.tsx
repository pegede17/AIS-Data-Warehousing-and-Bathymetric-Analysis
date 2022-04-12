import React from 'react';
import '../styles/app.scss';
import {BrowserRouter as Router, Route, Routes} from "react-router-dom";
import MainLayout from "./temp/_layout";
import {SidebarProvider} from "../contexts/sidebarContext";
import {SnackbarProvider} from 'notistack';

function App() {
    return (
        <Router>
            <SnackbarProvider>
                <SidebarProvider>
                    <Routes>
                        <Route path={'/'} element={<MainLayout/>}/>
                    </Routes>
                </SidebarProvider>
            </SnackbarProvider>
        </Router>
    );
}

export default App;
